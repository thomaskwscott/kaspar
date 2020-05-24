package uk.co.threefi.dataload;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class SegmentLoader implements Serializable {

  private static final String LOG_DIR="/var/lib/kafka/data";


  public static JavaRDD<String> getRawRows(JavaSparkContext jsc, String topicName,Properties clientProps) {
    AdminClient adminClient = AdminClient.create(clientProps);

    List<String> taskAssigments = new ArrayList<>();
    try {
      Map<String, TopicDescription> descriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all().get();
      Map<Integer,List<Integer>> brokerLeaderMappings = new HashMap<>();
      for(TopicPartitionInfo partition : descriptions.get(topicName).partitions()) {
        int leaderBroker = partition.leader().id();
        int partitionId = partition.partition();
        if(brokerLeaderMappings.containsKey(leaderBroker)) {
          brokerLeaderMappings.get(leaderBroker).add(partitionId);
        } else {
          brokerLeaderMappings.put(leaderBroker,new ArrayList<>(Arrays.asList(partitionId)));
        }
      }
      brokerLeaderMappings.forEach((k,v) -> taskAssigments.add(k +":"+ v.stream().map(i -> i.toString()).collect(Collectors.joining(","))));
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("bad thing happened");
    }

    JavaRDD<String> rawData = jsc.parallelize(Arrays.asList(taskAssigments.toArray(new String[taskAssigments.size()])))
            .repartition(3)
            .flatMap(i ->  {
              String expectedBrokerId = i.split(":")[0];
              String[] brokerHostedPartitions = i.split(":")[1].split(",");
              String actualbrokerId = getBrokerId();
              if(!expectedBrokerId.equals(actualbrokerId)) {
                throw new RuntimeException("Ignore this, Spark scheduled this task on the wrong broker. Expected: " +
                        expectedBrokerId + " actual: " + actualbrokerId + ". \n" +
                        "You should have blacklisting configurations that mean this will be rescheduled on a different node\n");
              }
              return getFileRecords(topicName,brokerHostedPartitions).iterator();
            });

    return rawData;
  }

  public static JavaRDD<Row> getRows(JavaRDD<String> rawData) {
    JavaRDD<Row> rows = rawData.map(new Function<String, Row>() {
      private static final long serialVersionUID = -812004521983071103L;
      public Row call(String rawData) {
        return RowFactory.create(Integer.valueOf(rawData));
      }});
    return rows;
  }

  private static String getBrokerId() {
    Properties props = new Properties();
    try {
      props.load(new FileInputStream("/etc/kafka/kafka.properties"));
    } catch (IOException e) {
      return "0";
    }
    return props.getProperty("broker.id");
  }

  private static List<String> getFileRecords(String topicName,String[] partitions) throws IOException {
    List<String> vals = new ArrayList<>();
    List<File> segmentsToRead = new ArrayList<>();

    for (String partition : partitions) {
      // get list of files
      File[] partitionFiles = new File(LOG_DIR + "/" + topicName + "-" + partition).listFiles(pathname -> pathname.getPath().endsWith(".log"));
      segmentsToRead.addAll(Arrays.asList(partitionFiles));
    }
    for(File segmentFile : segmentsToRead) { ;
      FileRecords records = FileRecords.open(segmentFile);

      Decoder<String> decoder = new StringDecoder(new VerifiableProperties());
      for (FileLogInputStream.FileChannelRecordBatch batch : records.batches()) {
        for (Record record : batch) {
          vals.add(decoder.fromBytes(Utils.readBytes(record.value())));
        }
      }
    }
    return vals;
  }

}
