
#!/usr/bin/env python3

import sys
import json
from datetime import datetime

for line in sys.stdin:
    try:
        transaction = json.loads(line.strip())

        customer_id = transaction["customer_id"]
        transaction_id = transaction["transaction_id"]
        total_amount = float(transaction["total_amount"])

        timestamp_str = transaction["timestamp"]

        try:
            timestamp = datetime.strptime(
                timestamp_str,
                "%Y-%m-%d %H:%M:%S.%f"
            )
        except ValueError:
            timestamp = datetime.strptime(
                timestamp_str,
                "%Y-%m-%d %H:%M:%S"
            )

        # Fenêtre fixe de 5 minutes
        bucket_minute = (timestamp.minute // 5) * 5

        window_start = timestamp.strftime(
            "%Y-%m-%d %H:"
        ) + "{:02d}".format(bucket_minute)

        # Clé : client + fenêtre
        key = "{}|{}".format(customer_id, window_start)

        # Valeur : transaction_id + montant
        value = "{}|{}".format(
            transaction_id,
            total_amount
        )

        print("{}\t{}".format(key, value))

    except Exception as e:
        print(
            "Erreur Mapper: {}".format(str(e)),
            file=sys.stderr
        )
