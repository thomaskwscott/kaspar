package uk.co.threefi.dataload;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Properties;

public class Main {

  public static final String BOOTSTRAP_SERVER = "worker1:9091";

  /* spark set up follows this:
  https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b
   */

  /* for spark-shell:
    import io.confluent.dataload.SegmentLoader
    val clientProps = new java.util.Properties
    clientProps.setProperty("bootstrap.servers","worker1:9091")
    val testRows = SegmentLoader.getRawRows(sc,"testTopic2",clientProps)
    val collectedRows = testRows.collect()
   */

  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf()
            .setAppName("SegmentRddLoader");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    Properties clientProps = new Properties();
    clientProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);



    JavaRDD<Row> testTopicRows = SegmentLoader.getRows(SegmentLoader.getRawRows(jsc,"testTopic",clientProps));
    JavaRDD<Row> testTopic2Rows = SegmentLoader.getRows(SegmentLoader.getRawRows(jsc,"testTopic2",clientProps));

    SQLContext sqlContext = new SQLContext(jsc);
    StructType schema = new StructType(new StructField[]{
            new StructField("col_int", DataTypes.IntegerType, false, Metadata.empty()),
    });

    Dataset<Row> testTopicDf = sqlContext.createDataFrame(testTopicRows,schema);
    testTopicDf.createOrReplaceTempView("testTopic");

    Dataset<Row> testTopic2Df = sqlContext.createDataFrame(testTopic2Rows,schema);
    testTopic2Df.createOrReplaceTempView("testTopic2");

    sqlContext.sql("select * from testTopic join testTopic2 on testTopic.col_int = testTopic2.col_int")
            .show(100);

  }
}
