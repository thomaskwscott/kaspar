#./launchExecutable.sh kaspar.properties
#curl -X POST localhost:8888/query --data-binary @/home/ubuntu/kaspar_server/src/main/resources/test_statement.sql
/usr/local/spark/bin/spark-submit --master "spark://master:7077" \
--num-executors 9 \
--executor-cores 1 \
--conf "spark.blacklist.enabled=true" \
--conf "spark.blacklist.killBlacklistedExecutors=false" \
--conf "spark.blacklist.timeout=1s" \
--conf "spark.blacklist.task.maxTaskAttemptsPerNode=1" \
--conf "spark.scheduler.blacklist.unschedulableTaskSetTimeout=1s" \
--conf "spark.driver.extraClassPath=../resources/external_libs/mysql-connector-java-8.0.23.jar" \
--jars ../kaspar/target/kaspar-1.0-SNAPSHOT.jar,../resources/external_libs/mysql-connector-java-8.0.23.jar \
--class kaspar.frontend.RestMain \
../kaspar_server/target/kaspar-server-1.0-SNAPSHOT.jar \
 -c $1