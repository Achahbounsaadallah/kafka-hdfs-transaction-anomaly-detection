
#!/bin/bash

set -Eeuo pipefail

STATE_DIR="/opt/airflow/admin-state"
BUILD_FILE="$STATE_DIR/last_build_id"
USER_FILE="$STATE_DIR/previous_username"
CREDENTIALS_FILE="$STATE_DIR/credentials.txt"

mkdir -p "$STATE_DIR"

echo "======================================"
echo "INITIALISATION AIRFLOW"
echo "======================================"

airflow db migrate

CURRENT_BUILD="${AIRFLOW_BUILD_ID:-dev}"
PREVIOUS_BUILD=""

if [ -f "$BUILD_FILE" ]; then
    PREVIOUS_BUILD=$(cat "$BUILD_FILE")
fi

if [ "$CURRENT_BUILD" != "$PREVIOUS_BUILD" ]; then

    echo "Nouvelle build détectée : $CURRENT_BUILD"

    RANDOM_ID=$(python -c "import secrets; print(secrets.token_hex(4))")
    NEW_USERNAME="admin_${RANDOM_ID}"
    NEW_PASSWORD=$(python -c "import secrets; print(secrets.token_urlsafe(24))")

    echo "Création du nouvel administrateur..."

    airflow users create \
        --username "$NEW_USERNAME" \
        --firstname "Airflow" \
        --lastname "Administrator" \
        --role Admin \
        --email "${NEW_USERNAME}@example.com" \
        --password "$NEW_PASSWORD"

    # Enregistrer les identifiants avant le nettoyage.
    umask 077

    printf '%s\n' "$NEW_USERNAME" > "$USER_FILE.tmp"
    mv "$USER_FILE.tmp" "$USER_FILE"

    {
        echo "Username: $NEW_USERNAME"
        echo "Password: $NEW_PASSWORD"
        echo "Build: $CURRENT_BUILD"
    } > "$CREDENTIALS_FILE.tmp"

    chmod 600 "$CREDENTIALS_FILE.tmp"
    mv "$CREDENTIALS_FILE.tmp" "$CREDENTIALS_FILE"

    # Enregistrer la build après la création réussie.
    printf '%s\n' "$CURRENT_BUILD" > "$BUILD_FILE.tmp"
    mv "$BUILD_FILE.tmp" "$BUILD_FILE"

    echo "Nouvel administrateur enregistré."

    # Supprimer tous les anciens comptes administrateurs
    # générés par le précédent script.
    airflow users list --output json > /tmp/airflow-users.json

    python - "$NEW_USERNAME" <<'PY'
import json
import subprocess
import sys

new_username = sys.argv[1]

with open("/tmp/airflow-users.json", encoding="utf-8") as f:
    users = json.load(f)

for user in users:
    username = user.get("username", "")

    if username.startswith("admin_") and username != new_username:
        print(f"Suppression de l'ancien compte : {username}")

        subprocess.run(
            ["airflow", "users", "delete", "--username", username],
            check=True
        )
PY

    OLD_USERNAME="${AIRFLOW_INITIAL_ADMIN_USERNAME:-}"

    if [ -n "$OLD_USERNAME" ] && [ "$OLD_USERNAME" != "$NEW_USERNAME" ]; then
        airflow users delete --username "$OLD_USERNAME" || true
    fi

    echo "Rotation des comptes terminée."

else
    echo "Même build : conservation du compte administrateur."
fi

echo "======================================"
echo "DEMARRAGE AIRFLOW"
echo "======================================"

airflow scheduler &
SCHEDULER_PID=$!

airflow webserver &
WEBSERVER_PID=$!

set +e
wait -n "$SCHEDULER_PID" "$WEBSERVER_PID"
STATUS=$?
set -e

kill "$SCHEDULER_PID" "$WEBSERVER_PID" 2>/dev/null || true

wait "$SCHEDULER_PID" 2>/dev/null || true
wait "$WEBSERVER_PID" 2>/dev/null || true

exit "$STATUS"
