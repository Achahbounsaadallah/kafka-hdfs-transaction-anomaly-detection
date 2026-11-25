
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

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactions")

GROUP_ID = os.getenv(
    "GROUP_ID",
    "transactions-hdfs-group"
)

EXPECTED_TRANSACTIONS = int(
    os.getenv("EXPECTED_TRANSACTIONS", "10000")
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "10"))

CONSUMER_IDLE_TIMEOUT = int(
    os.getenv("CONSUMER_IDLE_TIMEOUT", "60")
)

HDFS_CONTAINER = os.getenv("HDFS_CONTAINER", "namenode")

HDFS_DIRECTORY = os.getenv(
    "HDFS_DIRECTORY",
    "/data/transactions"
)

ASSIGNMENT_TIMEOUT = 60


# ==========================================
# 2. CONNEXION KAFKA
# ==========================================

consumer = KafkaConsumer(

    bootstrap_servers=KAFKA_SERVER,

    group_id=GROUP_ID,

    auto_offset_reset="latest",

    enable_auto_commit=False,

    session_timeout_ms=10000,

    heartbeat_interval_ms=3000,

    request_timeout_ms=30000,

    max_poll_interval_ms=300000,

    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


# ==========================================
# 3. EXECUTION DES COMMANDES DOCKER
# ==========================================

def run_command(command):

    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True
    )

    return result.stdout


# ==========================================
# 4. PREPARATION HDFS
# ==========================================

run_command([
    "docker",
    "exec",
    HDFS_CONTAINER,
    "hdfs",
    "dfs",
    "-mkdir",
    "-p",
    HDFS_DIRECTORY
])

print(
    "Repertoire HDFS pret :",
    HDFS_DIRECTORY,
    flush=True
)


# ==========================================
# 5. PREPARATION DU CONSUMER
# ==========================================

consumer.subscribe([TOPIC_NAME])

print(
    "Attente de l'affectation des partitions Kafka...",
    flush=True
)

assignment_deadline = time.monotonic() + ASSIGNMENT_TIMEOUT

while not consumer.assignment():

    consumer.poll(timeout_ms=1000)

    assigned_partitions = consumer.assignment()

    print(
        "Partitions actuellement affectees :",
        assigned_partitions,
        flush=True
    )

    if time.monotonic() >= assignment_deadline:

        raise TimeoutError(
            "Impossible d'obtenir les partitions Kafka. "
            "Topic : {} | Groupe : {}".format(
                TOPIC_NAME,
                GROUP_ID
            )
        )


# Positionner le Consumer apres les anciens messages.
consumer.seek_to_end()

# Lire les positions initiales apres l'affectation.
positions = {}

for partition in consumer.assignment():

    positions[partition] = consumer.position(partition)

print(
    "Partitions affectees :",
    consumer.assignment(),
    flush=True
)

print(
    "Positions initiales :",
    positions,
    flush=True
)

print("CONSUMER_READY", flush=True)


# ==========================================
# 6. VARIABLES DU BATCH
# ==========================================

batch = []

batch_start_time = None

received_count = 0

last_message_time = time.monotonic()


# ==========================================
# 7. ARCHIVAGE D'UN BATCH DANS HDFS
# ==========================================

def archive_batch(records):

    if not records:
        return

    offsets_by_partition = {}

    for record in records:

        partition = record["partition"]

        offset = record["offset"]

        offsets_by_partition.setdefault(
            partition,
            []
        ).append(offset)

    # Creer un nom unique a partir des offsets.

    filename_parts = []

    for partition in sorted(offsets_by_partition):

        offsets = offsets_by_partition[partition]

        filename_parts.append(
            "p{}_{}-{}".format(
                partition,
                min(offsets),
                max(offsets)
            )
        )

    filename = (
        "transactions_"
        + "_".join(filename_parts)
        + ".jsonl"
    )

    temp_file_path = None

    try:

        # A. Creer un fichier JSONL local.

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".jsonl",
            prefix="transactions_",
            delete=False
        ) as temp_file:

            temp_file_path = temp_file.name

            for record in records:

                json_line = json.dumps(
                    record["value"],
                    ensure_ascii=False
                )

                temp_file.write(json_line + "\n")

        # B. Copier dans le conteneur NameNode.

        run_command([
            "docker",
            "cp",
            temp_file_path,
            "{}:/tmp/{}".format(
                HDFS_CONTAINER,
                filename
            )
        ])

        # C. Enregistrer dans HDFS.

        hdfs_file_path = "{}/{}".format(
            HDFS_DIRECTORY,
            filename
        )

        run_command([
            "docker",
            "exec",
            HDFS_CONTAINER,
            "hdfs",
            "dfs",
            "-put",
            "-f",
            "/tmp/{}".format(filename),
            hdfs_file_path
        ])

        print(
            "Batch archive dans HDFS : {} ({} transactions)".format(
                filename,
                len(records)
            ),
            flush=True
        )

        # D. Valider les offsets apres l'archivage.

        offsets_to_commit = {}

        for partition, offsets in offsets_by_partition.items():

            topic_partition = TopicPartition(
                TOPIC_NAME,
                partition
            )

            next_offset = max(offsets) + 1

            offsets_to_commit[topic_partition] = (
                OffsetAndMetadata(next_offset, "")
            )

        consumer.commit(
            offsets=offsets_to_commit
        )

        print(
            "Offsets Kafka valides :",
            offsets_to_commit,
            flush=True
        )

    finally:

        # E. Nettoyage des fichiers temporaires.

        try:

            run_command([
                "docker",
                "exec",
                HDFS_CONTAINER,
                "rm",
                "-f",
                "/tmp/{}".format(filename)
            ])

        except subprocess.CalledProcessError:

            print(
                "Attention : nettoyage temporaire impossible.",
                flush=True
            )

        if temp_file_path and os.path.exists(temp_file_path):

            os.remove(temp_file_path)


# ==========================================
# 8. CONSOMMATION DES TRANSACTIONS
# ==========================================

print(
    "Consumer en attente des nouvelles transactions...",
    flush=True
)

try:

    while received_count < EXPECTED_TRANSACTIONS:

        remaining = EXPECTED_TRANSACTIONS - received_count

        available_in_batch = BATCH_SIZE - len(batch)

        max_records = max(
            1,
            min(available_in_batch, remaining)
        )

        messages = consumer.poll(
            timeout_ms=1000,
            max_records=max_records
        )

        # Parcourir les messages recus.

        for topic_partition, records in messages.items():

            for message in records:

                if received_count >= EXPECTED_TRANSACTIONS:
                    break

                batch.append({

                    "value": message.value,

                    "partition": message.partition,

                    "offset": message.offset

                })

                received_count += 1

                last_message_time = time.monotonic()

                if batch_start_time is None:

                    batch_start_time = time.monotonic()

                if received_count % 100 == 0:

                    print(
                        "Transactions recues : {}/{}".format(
                            received_count,
                            EXPECTED_TRANSACTIONS
                        ),
                        flush=True
                    )

        # Verifier le timeout d'inactivite.

        if (
            time.monotonic() - last_message_time
            > CONSUMER_IDLE_TIMEOUT
        ):

            raise TimeoutError(
                "Aucune nouvelle transaction recue depuis {} secondes."
                .format(CONSUMER_IDLE_TIMEOUT)
            )

        # Calculer le temps ecoule depuis le debut du batch.

        elapsed_time = (

            time.monotonic() - batch_start_time

            if batch_start_time is not None

            else 0

        )

        batch_is_full = len(batch) >= BATCH_SIZE

        timeout_reached = (

            batch_start_time is not None

            and elapsed_time >= BATCH_TIMEOUT

        )

        # Archiver le batch.

        if batch and (batch_is_full or timeout_reached):

            print(
                "Debut archivage batch...",
                flush=True
            )

            archive_batch(batch)

            batch.clear()

            batch_start_time = None

    # ======================================
    # 9. ARCHIVAGE DU DERNIER BATCH
    # ======================================

    if batch:

        print(
            "Archivage du dernier batch incomplet...",
            flush=True
        )

        archive_batch(batch)

        batch.clear()

    print("===================================", flush=True)

    print("CONSOMMATION TERMINEE", flush=True)

    print(
        "Transactions recues :",
        received_count,
        flush=True
    )

    print("===================================", flush=True)


except Exception as error:

    print(
        "ERREUR CONSUMER :",
        str(error),
        flush=True
    )

    raise


finally:

    consumer.close()

    print(
        "Consumer ferme.",
        flush=True
    )
