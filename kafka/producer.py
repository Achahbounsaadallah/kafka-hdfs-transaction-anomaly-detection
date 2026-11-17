
import json
import time
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ==========================================
# 1. CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "transactions"

CSV_PATH = "data/raw/transactions.csv"

# Delai entre deux transactions
SEND_INTERVAL = 0.1


# ==========================================
# 2. CONNEXION AU BROKER KAFKA
# ==========================================

producer = KafkaProducer(

    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda value: json.dumps(
        value,
        ensure_ascii=False
    ).encode("utf-8"),

    key_serializer=lambda key: key.encode("utf-8"),

    acks="all",

    retries=5,

    max_in_flight_requests_per_connection=1
)


# ==========================================
# 3. LECTURE DU CSV
# ==========================================

df = pd.read_csv(CSV_PATH)

print("===================================")
print("KAFKA PRODUCER DEMARRE")
print("===================================")

print(f"Nombre total de transactions : {len(df)}")


# ==========================================
# 4. EXCLURE LES ETIQUETTES
# ==========================================

label_columns = [
    "is_anomaly",
    "anomaly_type"
]

df = df.drop(
    columns=label_columns,
    errors="ignore"
)

print("\nColonnes envoyees :")

print(df.columns.tolist())


# ==========================================
# 5. ENVOI DES TRANSACTIONS
# ==========================================

sent_count = 0

try:

    for _, row in df.iterrows():

        # Transformer la ligne en dictionnaire
        transaction = row.to_dict()

        # Utiliser transaction_id comme cle Kafka
        transaction_key = str(
            transaction["transaction_id"]
        )

        # Envoyer la transaction dans Kafka
        future = producer.send(
            TOPIC_NAME,
            key=transaction_key,
            value=transaction
        )

        # Attendre la confirmation du broker
        metadata = future.get(timeout=30)

        sent_count += 1

        print(
            f"[{sent_count}/{len(df)}] "
            f"Transaction envoyee : "
            f"{transaction_key} | "
            f"Partition : {metadata.partition} | "
            f"Offset : {metadata.offset}"
        )

        # Simuler un flux de transactions
        time.sleep(SEND_INTERVAL)


    # Attendre l'envoi de tous les messages restants
    producer.flush()

    print("\n===================================")
    print("ENVOI TERMINE")
    print("===================================")

    print(f"Transactions envoyees : {sent_count}")


except KeyboardInterrupt:

    print("\nArret demande par l'utilisateur.")

except KafkaError as error:

    print(f"Erreur Kafka : {error}")

except Exception as error:

    print(f"Erreur : {error}")

finally:

    producer.flush()
    producer.close()

    print("Producer ferme.")
