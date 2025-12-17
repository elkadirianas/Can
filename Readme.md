docker-compose build --no-cache

docker-compose up -d zookeeper kafka init-kafka spark-processor


#create a consumer to watch the msgs geting produced to the kafka-topic live
docker exec can-kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic player_metrics --from-beginning

#RUN the Coach and see substituion happens live :

docker-compose run --rm coach-cli

