for workerNum in {1..3}
do
  echo Creating index on worker ${workerNum} for Customers partition 0
  docker-compose exec worker${workerNum} bash -c 'echo "5:11:112" > /var/lib/kafka/data/Customers-0/00000000000000000000.minMax.index'
  echo Creating index on worker ${workerNum} for Customers partition 1
  docker-compose exec worker${workerNum} bash -c 'echo "5:11:112" > /var/lib/kafka/data/Customers-1/00000000000000000000.minMax.index'
  echo Creating index on worker ${workerNum} for Customers partition 2
  docker-compose exec worker${workerNum} bash -c 'echo "5:11:112" > /var/lib/kafka/data/Customers-2/00000000000000000000.minMax.index'
  echo Creating index on worker ${workerNum} for Customers partition 3
  docker-compose exec worker${workerNum} bash -c 'echo "5:11:112" > /var/lib/kafka/data/Customers-3/00000000000000000000.minMax.index'
  echo Creating index on worker ${workerNum} for Customers partition 4
  docker-compose exec worker${workerNum} bash -c 'echo "5:11:112" > /var/lib/kafka/data/Customers-4/00000000000000000000.minMax.index'
  echo Creating index on worker ${workerNum} for Customers partition 5
  docker-compose exec worker${workerNum} bash -c 'echo "5:11:112" > /var/lib/kafka/data/Customers-5/00000000000000000000.minMax.index'
done