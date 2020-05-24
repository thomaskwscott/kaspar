# kaspar
Kafka/Spark integration

Build images with spark worker/confluent:

cd docker
./make-images.sh

To run the demo:

docker-compose exec master bash
cd /home/ubuntu
./setup.sh
./launchDemo.sh
