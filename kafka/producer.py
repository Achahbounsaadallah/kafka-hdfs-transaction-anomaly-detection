
import json
import os
import time
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ==========================================
# 1. CONFIGURATION
# ==========================================

KAFKA_SERVER = os.getenv(
    "KAFKA_SERVER",
    "kafka:9092"
)

TOPIC_NAME = "transactions"

CSV_PATH = os.getenv(
    "CSV_PATH",
    "/opt/airflow/project/data/raw/transactions.csv"
)

MAX_TRANSACTIONS = int(
    os.getenv("MAX_TRANSACTIONS", "10000")
)

SEND_INTERVAL = float(
    os.getenv("SEND_INTERVAL", "0.1")
)


# ==========================================
# 2. CONNEXION KAFKA
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

try:

    df = pd.read_csv(CSV_PATH)

    if MAX_TRANSACTIONS <= 0:
        raise ValueError("MAX_TRANSACTIONS doit etre positif.")

    if len(df) < MAX_TRANSACTIONS:
        raise ValueError(
            "Le CSV contient seulement {} transactions, "
            "mais {} sont demandees.".format(
                len(df), MAX_TRANSACTIONS
            )
        )

    df = df.head(MAX_TRANSACTIONS)

    # Exclure les colonnes d'etiquetage.
    label_columns = [
        "is_anomaly",
        "anomaly_type"
    ]

    df = df.drop(
        columns=label_columns,
        errors="ignore"
    )

    print("===================================")
    print("KAFKA PRODUCER DEMARRE")
    print("===================================")

    print("Transactions a envoyer :", len(df))

    print("Colonnes envoyees :")
    print(df.columns.tolist())


    # ==========================================
    # 4. ENVOI DES TRANSACTIONS
    # ==========================================

    sent_count = 0

    for _, row in df.iterrows():

        transaction = {}

        for column, value in row.items():

            if pd.isna(value):
                value = None

            elif hasattr(value, "item"):
                value = value.item()

            transaction[column] = value

        transaction_key = str(
            transaction["transaction_id"]
        )

        future = producer.send(
            TOPIC_NAME,
            key=transaction_key,
            value=transaction
        )

        metadata = future.get(timeout=30)

        sent_count += 1

        if sent_count % 100 == 0 or sent_count == len(df):

            print(
                "[{}/{}] Transaction envoyee | "
                "Partition={} | Offset={}".format(
                    sent_count,
                    len(df),
                    metadata.partition,
                    metadata.offset
                )
            )

        time.sleep(SEND_INTERVAL)

    producer.flush()

    print("===================================")
    print("ENVOI TERMINE")
    print("Transactions envoyees :", sent_count)
    print("===================================")

except Exception as error:

    print("ERREUR PRODUCER :", str(error))

    raise

finally:

    producer.flush()
    producer.close()

    print("Producer ferme.")
