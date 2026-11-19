
#!/usr/bin/env python3

import sys
import json
from datetime import datetime, timedelta

THRESHOLD = 100

current_key = None
current_count = 0
current_total_amount = 0.0


def emit_anomaly(key, count, amount):

    if count >= THRESHOLD:

        customer_id, window_start = key.split("|", 1)

        start_datetime = datetime.strptime(
            window_start,
            "%Y-%m-%d %H:%M"
        )

        end_datetime = start_datetime + timedelta(minutes=5)

        anomaly = {
            "anomaly_type": "HIGH_FREQUENCY",
            "customer_id": customer_id,
            "window_start": window_start,
            "window_end": end_datetime.strftime(
                "%Y-%m-%d %H:%M"
            ),
            "transaction_count": count,
            "total_amount": round(amount, 2),
            "threshold": THRESHOLD,
            "anomaly_reason": (
                "Plus de 100 transactions dans une fenetre de 5 minutes"
            )
        }

        print(json.dumps(anomaly))


for line in sys.stdin:

    try:
        parts = line.strip().split("\t", 1)

        if len(parts) != 2:
            continue

        key = parts[0]

        # Récupérer transaction_id et montant
        value_parts = parts[1].split("|", 1)

        if len(value_parts) != 2:
            continue

        transaction_id = value_parts[0]
        amount = float(value_parts[1])

        if key == current_key:

            current_count += 1
            current_total_amount += amount

        else:

            # Terminer le groupe précédent
            if current_key is not None:
                emit_anomaly(
                    current_key,
                    current_count,
                    current_total_amount
                )

            # Initialiser le nouveau groupe
            current_key = key
            current_count = 1
            current_total_amount = amount

    except Exception as e:
        print(
            "Erreur Reducer: {}".format(str(e)),
            file=sys.stderr
        )


# Traiter le dernier groupe
if current_key is not None:

    emit_anomaly(
        current_key,
        current_count,
        current_total_amount
    )
