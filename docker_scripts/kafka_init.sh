until [ $(kafka-broker-api-versions --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    2>/dev/null | grep -c 'id:') -eq 3 ]; do
    echo "Waiting for all 3 brokers..."
    sleep 2
done

echo "All brokers detected."

until kafka-topics --create --if-not-exists --topic __healthcheck \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --partitions 1 --replication-factor 3; do
    echo "Cluster not ready for replication yet..."
    sleep 2
done

echo "Cluster is ready."

until kafka-topics --create --if-not-exists --topic trades.normalized \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --partitions 3 --replication-factor 3 --config retention.ms=86400000; do
    echo "Retrying topic creation..."
    sleep 2
done

until kafka-topics --create --if-not-exists --topic trades.raw \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --partitions 3 --replication-factor 3 --config retention.ms=86400000; do
    echo "Retrying topic creation..."
    sleep 2
done

echo "Topics created successfully."
