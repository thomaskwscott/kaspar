/usr/local/spark/bin/spark-shell --master "spark://master:7077" \
--num-executors 9 \
--executor-cores 1 \
--conf "spark.blacklist.enabled=true" \
--conf "spark.blacklist.killBlacklistedExecutors=false" \
--conf "spark.blacklist.timeout=1s" \
--conf "spark.blacklist.task.maxTaskAttemptsPerNode=1" \
--conf "spark.scheduler.blacklist.unschedulableTaskSetTimeout=1s" \
--jars ../demo/target/kaspar-1.0-SNAPSHOT.jar