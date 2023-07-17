
# Запускаем инфраструктуру
cd example/kafka_connect
docker-compose up

# Загружаем postgres connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json

# При помощи kafka-console-consumer считываем все сообщения из топика dbserver1.inventory.customers
  docker-compose -f docker-compose.yml exec kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --from-beginning \
  --property print.key=true \
  --topic dbserver1.inventory.customers

# Модифицируем записи в таблице inventory.customers, изменения будут считаны kafka-consumer'ом
docker-compose -f docker-compose.yml exec postgres env PGOPTIONS="--search_path=inventory" bash -c 'psql -U $POSTGRES_USER postgres'
  
