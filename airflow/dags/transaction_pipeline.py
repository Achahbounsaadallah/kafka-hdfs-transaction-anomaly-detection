
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT = "/opt/airflow/project"

NAMENODE = "namenode"

HADOOP_STREAMING = (
    "/opt/hadoop-3.2.1/share/hadoop/tools/lib/"
    "hadoop-streaming-3.2.1.jar"
)


with DAG(

    dag_id="kafka_hdfs_anomaly_pipeline",

    description="Kafka ingestion, HDFS storage and MapReduce anomaly detection",

    start_date=datetime(2026, 11, 1),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=["Kafka", "HDFS", "Hadoop", "MapReduce"],

) as dag:


    # ==========================================
    # 1. KAFKA INGESTION
    # PRODUCER + CONSUMER EN PARALLELE
    # ==========================================

    run_ingestion = BashOperator(

        task_id="run_kafka_ingestion",

        bash_command=f"""
        set -euo pipefail

        cd {PROJECT}

        RUN_ID="{{{{ ts_nodash }}}}"

        export GROUP_ID="transactions-hdfs-${{RUN_ID}}"

        export HDFS_DIRECTORY="/data/transactions/${{RUN_ID}}"

        export EXPECTED_TRANSACTIONS=10000

        export MAX_TRANSACTIONS=10000

        echo "==================================="
        echo "DEMARRAGE DE L'INGESTION"
        echo "==================================="

        # Demarrer le Consumer en arriere-plan.

        python -u kafka/consumer.py > /tmp/consumer.log 2>&1 &

        CONSUMER_PID=$!

        # Attendre que le Consumer soit pret.

        READY=0

        for i in $(seq 1 60); do

            if grep -q "CONSUMER_READY" /tmp/consumer.log; then

                READY=1
                break

            fi

            if ! kill -0 $CONSUMER_PID 2>/dev/null; then

                echo "Le Consumer a echoue."

                cat /tmp/consumer.log

                exit 1

            fi

            sleep 1

        done

        if [ "$READY" -ne 1 ]; then

            echo "Timeout : le Consumer n'est pas pret."

            cat /tmp/consumer.log

            exit 1

        fi

        echo "Consumer pret. Demarrage du Producer."

        # Lancer le Producer.

        python -u kafka/producer.py

        echo "Production terminee."

        # Attendre la fin du Consumer.

        wait $CONSUMER_PID

        echo "Consumer termine avec succes."

        cat /tmp/consumer.log

        echo "INGESTION TERMINEE."
        """,

        retries=0,

    )


    # ==========================================
    # 2. PREPARATION DES SCRIPTS MAPREDUCE
    # ==========================================

    prepare_mapreduce = BashOperator(

        task_id="prepare_mapreduce",

        bash_command=f"""
        set -euo pipefail

        echo "Preparation des scripts MapReduce..."

        docker exec {NAMENODE} mkdir -p /tmp/mapreduce

        docker cp \
        {PROJECT}/mapreduce/anomaly-detection/mapper.py \
        {NAMENODE}:/tmp/mapreduce/mapper.py

        docker cp \
        {PROJECT}/mapreduce/anomaly-detection/reducer.py \
        {NAMENODE}:/tmp/mapreduce/reducer.py

        docker cp \
        {PROJECT}/mapreduce/frequency-detection/mapper_frequency.py \
        {NAMENODE}:/tmp/mapreduce/mapper_frequency.py

        docker cp \
        {PROJECT}/mapreduce/frequency-detection/reducer_frequency.py \
        {NAMENODE}:/tmp/mapreduce/reducer_frequency.py

        echo "Scripts MapReduce copies avec succes."
        """,

    )


    # ==========================================
    # 3. DETECTION DES ANOMALIES INDIVIDUELLES
    # HIGH_AMOUNT + PRICE_MISMATCH
    # ==========================================

    detect_individual_anomalies = BashOperator(

        task_id="detect_individual_anomalies",

        bash_command=f"""
        set -euo pipefail

        RUN_ID="{{{{ ts_nodash }}}}"

        INPUT="/data/transactions/${{RUN_ID}}"

        OUTPUT="/data/anomaly-reports/${{RUN_ID}}/individual-rules"

        echo "Demarrage du job anomalies individuelles..."

        docker exec {NAMENODE} hadoop jar \
        {HADOOP_STREAMING} \
        -D mapreduce.job.reduces=1 \
        -files /tmp/mapreduce/mapper.py,/tmp/mapreduce/reducer.py \
        -mapper "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -input "$INPUT" \
        -output "$OUTPUT"

        echo "Anomalies individuelles detectees."

        docker exec {NAMENODE} hdfs dfs -ls "$OUTPUT"
        """,

    )


    # ==========================================
    # 4. DETECTION HIGH FREQUENCY
    # ==========================================

    detect_high_frequency = BashOperator(

        task_id="detect_high_frequency",

        bash_command=f"""
        set -euo pipefail

        RUN_ID="{{{{ ts_nodash }}}}"

        INPUT="/data/transactions/${{RUN_ID}}"

        OUTPUT="/data/anomaly-reports/${{RUN_ID}}/high-frequency"

        echo "Demarrage du job HIGH_FREQUENCY..."

        docker exec {NAMENODE} hadoop jar \
        {HADOOP_STREAMING} \
        -D mapreduce.job.reduces=1 \
        -files /tmp/mapreduce/mapper_frequency.py,/tmp/mapreduce/reducer_frequency.py \
        -mapper "python3 mapper_frequency.py" \
        -reducer "python3 reducer_frequency.py" \
        -input "$INPUT" \
        -output "$OUTPUT"

        echo "Detection HIGH_FREQUENCY terminee."

        docker exec {NAMENODE} hdfs dfs -ls "$OUTPUT"
        """,

    )


    # ==========================================
    # 5. DEPENDANCES DU PIPELINE
    # ==========================================

    run_ingestion >> prepare_mapreduce

    prepare_mapreduce >> detect_individual_anomalies

    detect_individual_anomalies >> detect_high_frequency
