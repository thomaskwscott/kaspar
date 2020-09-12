if [ ! -f resources/spark-3.0.0-bin-hadoop2.7.tgz ]; then
  rm -rf resources/spark-3.0.0-bin-hadoop2.7
  wget --no-check-certificate -P resources https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
  tar -xvf resources/spark-3.0.0-bin-hadoop2.7.tgz -C resources
fi

docker build --tag kaspar-master:1.0 -f master/Dockerfile .
docker build --tag kaspar-worker:1.0 -f worker/Dockerfile .