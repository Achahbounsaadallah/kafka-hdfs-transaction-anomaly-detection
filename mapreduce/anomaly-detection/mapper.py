
#!/usr/bin/env python3
import sys
import json

# Seuil initial de detection
AMOUNT_THRESHOLD = 10000


def detect_anomalies(transaction):
    anomalies = []

    total_amount = float(transaction.get("total_amount", 0))
    unit_price = float(transaction.get("unit_price", 0))
    reference_price = float(transaction.get("reference_price", 0))

    # Regle 1 : montant eleve
    if total_amount > AMOUNT_THRESHOLD:
        anomaly = transaction.copy()
        anomaly["anomaly_type"] = "HIGH_AMOUNT"
        anomaly["anomaly_reason"] = (
            "Montant total superieur a {}".format(AMOUNT_THRESHOLD)
        )
        anomalies.append(anomaly)

    # Regle 2 : prix unitaire superieur au prix de reference
    if unit_price > reference_price:
        anomaly = transaction.copy()
        anomaly["anomaly_type"] = "PRICE_MISMATCH"
        anomaly["anomaly_reason"] = (
            "Prix unitaire superieur au prix de reference"
        )
        anomalies.append(anomaly)

    return anomalies


for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    try:
        transaction = json.loads(line)
        anomalies = detect_anomalies(transaction)

        for anomaly in anomalies:
            print("ANOMALY\t{}".format(json.dumps(anomaly)))

    except (ValueError, TypeError, KeyError) as error:
        print("Erreur de lecture JSON : {}".format(error), file=sys.stderr)
