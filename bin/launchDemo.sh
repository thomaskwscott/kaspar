/usr/local/spark/bin/spark-submit --master "spark://master:7077" \
--class uk.co.threefi.dataload.Main \
--num-executors 3 \
--executor-cores 1 \
--conf "spark.blacklist.enabled=true" \
--conf "spark.blacklist.killBlacklistedExecutors=false" \
--conf "spark.blacklist.timeout=1s" \
--conf "spark.blacklist.task.maxTaskAttemptsPerNode=1" \
--conf "spark.scheduler.blacklist.unschedulableTaskSetTimeout=1s" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files `pwd`/log4j.properties \
../demo/target/kaspar-1.0-SNAPSHOT.jar