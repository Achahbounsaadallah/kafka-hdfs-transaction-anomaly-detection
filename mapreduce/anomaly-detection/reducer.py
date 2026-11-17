
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    try:
        key, anomaly_json = line.split("\t", 1)

        if key == "ANOMALY":
            print(anomaly_json)

    except ValueError:
        print("Ligne invalide : {}".format(line), file=sys.stderr)
