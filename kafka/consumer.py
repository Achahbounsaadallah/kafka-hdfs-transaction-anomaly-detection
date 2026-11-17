
import json
import os
import tempfile
import time
import subprocess

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata


# ==========================================
# 1. CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "transactions"
GROUP_ID = "transactions-hdfs-group"

BATCH_SIZE = 500
BATCH_TIMEOUT = 10  # secondes

HDFS_CONTAINER = "namenode"
HDFS_DIRECTORY = "/data/transactions"


# ==========================================
# 2. CONNEXION A KAFKA
# ==========================================

consumer = KafkaConsumer(
    TOPIC_NAME,

    bootstrap_servers=KAFKA_SERVER,

    group_id=GROUP_ID,

    # Lire depuis le début si aucun offset
    # n'a encore été enregistré pour ce groupe.
    auto_offset_reset="earliest",

    # Nous validons les offsets manuellement
    # après l'archivage réussi dans HDFS.
    enable_auto_commit=False,

    # Transformer automatiquement le JSON en dictionnaire Python.
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


# ==========================================
# 3. FONCTION POUR EXECUTER DOCKER
# ==========================================

def run_command(command):
    """Execute une commande et signale toute erreur."""

    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True
    )

    return result.stdout


# ==========================================
# 4. PREPARER LE REPERTOIRE HDFS
# ==========================================

run_command([
    "docker", "exec", HDFS_CONTAINER,
    "hdfs", "dfs", "-mkdir", "-p", HDFS_DIRECTORY
])

print("Connexion Kafka reussie.")
print("Repertoire HDFS pret :", HDFS_DIRECTORY)


# ==========================================
# 5. VARIABLES DU BATCH
# ==========================================

batch = []
batch_start_time = None


# ==========================================
# 6. ARCHIVAGE D'UN BATCH DANS HDFS
# ==========================================

def archive_batch(records):

    if not records:
        return

    # Calculer les plages d'offsets par partition.
    offsets_by_partition = {}

    for record in records:
        partition = record["partition"]
        offset = record["offset"]

        offsets_by_partition.setdefault(
            partition, []
        ).append(offset)

    # Construire un nom déterministe à partir des offsets.
    # Cela permet de réutiliser le même fichier si le lot
    # est rejoué après un échec de validation Kafka.
    filename_parts = []

    for partition in sorted(offsets_by_partition):
        offsets = offsets_by_partition[partition]

        filename_parts.append(
            f"p{partition}_{min(offsets)}-{max(offsets)}"
        )

    filename = "transactions_" + "_".join(filename_parts) + ".jsonl"

    # --------------------------------------
    # A. CREATION DU FICHIER JSONL LOCAL
    # --------------------------------------

    temp_file_path = None

    try:

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".jsonl",
            prefix="transactions_",
            delete=False
        ) as temp_file:

            temp_file_path = temp_file.name

            for record in records:

                # Une transaction JSON par ligne.
                json_line = json.dumps(
                    record["value"],
                    ensure_ascii=False
                )

                temp_file.write(json_line + "\n")

        print(
            f"\nFichier temporaire cree : {temp_file_path}"
        )

        # --------------------------------------
        # B. COPIE VERS LE CONTENEUR NAMENODE
        # --------------------------------------

        run_command([
            "docker", "cp",
            temp_file_path,
            f"{HDFS_CONTAINER}:/tmp/{filename}"
        ])

        print("Fichier copie dans le conteneur NameNode.")

        # --------------------------------------
        # C. ARCHIVAGE DANS HDFS
        # --------------------------------------

        run_command([
            "docker", "exec", HDFS_CONTAINER,
            "hdfs", "dfs",
            "-put", "-f",
            f"/tmp/{filename}",
            f"{HDFS_DIRECTORY}/{filename}"
        ])

        print("Batch archive avec succes dans HDFS.")

        # --------------------------------------
        # D. VALIDATION DES OFFSETS KAFKA
        # --------------------------------------

        offsets_to_commit = {}

        for partition, offsets in offsets_by_partition.items():

            topic_partition = TopicPartition(
                TOPIC_NAME,
                partition
            )

            # Kafka attend le prochain offset à lire.
            next_offset = max(offsets) + 1

            offsets_to_commit[topic_partition] = (
                OffsetAndMetadata(next_offset, "")
            )

        consumer.commit(offsets=offsets_to_commit)

        print("Offsets Kafka valides avec succes.")

        # --------------------------------------
        # E. NETTOYAGE DU FICHIER TEMPORAIRE
        # --------------------------------------

        try:
            run_command([
                "docker", "exec", HDFS_CONTAINER,
                "hdfs", "dfs", "-rm", "-f",
                f"/tmp/{filename}"
            ])
        except subprocess.CalledProcessError:
            print("Attention : impossible de supprimer la copie temporaire HDFS.")

        print(
            f"Nombre de transactions archivees : {len(records)}"
        )

    finally:

        # Supprimer le fichier temporaire Windows.
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)


# ==========================================
# 7. BOUCLE PRINCIPALE DU CONSUMER
# ==========================================

print("\nConsumer en attente de transactions...\n")

try:

    while True:

        # Recuperer les messages disponibles.
        remaining = BATCH_SIZE - len(batch)

        messages = consumer.poll(
            timeout_ms=1000,
            max_records=max(1, remaining)
        )

        # Ajouter les messages au batch.
        for topic_partition, records in messages.items():

            for message in records:

                batch.append({
                    "value": message.value,
                    "partition": message.partition,
                    "offset": message.offset
                })

                if batch_start_time is None:
                    batch_start_time = time.monotonic()

                print(
                    f"Transaction recue : "
                    f"{message.value.get('transaction_id')} "
                    f"| Partition : {message.partition} "
                    f"| Offset : {message.offset}"
                )

        # Calculer le temps ecoule depuis le premier message.
        elapsed_time = (
            time.monotonic() - batch_start_time
            if batch_start_time is not None
            else 0
        )

        # Condition 1 : 500 transactions.
        batch_is_full = len(batch) >= BATCH_SIZE

        # Condition 2 : 10 secondes ecoulees.
        timeout_reached = (
            batch_start_time is not None
            and elapsed_time >= BATCH_TIMEOUT
        )

        if batch and (batch_is_full or timeout_reached):

            print("\n================================")
            print("DEBUT DE L'ARCHIVAGE DU BATCH")
            print("================================")

            archive_batch(batch)

            # Vider le batch uniquement après archivage
            # et validation réussis.
            batch.clear()
            batch_start_time = None

            print("\nConsumer en attente du prochain batch...\n")


except KeyboardInterrupt:

    print("\nArret demande par l'utilisateur.")

    # Archiver les messages restants avant de quitter.
    if batch:
        print("Archivage du dernier batch...")
        archive_batch(batch)
        batch.clear()


finally:

    consumer.close()
    print("Consumer ferme.")
