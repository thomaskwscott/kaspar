if [ ! -f resources/spark-2.4.5-bin-hadoop2.7.tgz ]; then
  rm -rf resources/spark-2.4.5-bin-hadoop2.7
  wget -P resources http://mirror.ox.ac.uk/sites/rsync.apache.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
  tar -xvf resources/spark-2.4.5-bin-hadoop2.7.tgz -C resources
fi

docker build --tag kaspar-master:1.0 -f master/Dockerfile .
docker build --tag kaspar-worker:1.0 -f worker/Dockerfile .