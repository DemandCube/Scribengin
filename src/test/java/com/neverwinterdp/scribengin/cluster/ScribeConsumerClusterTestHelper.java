package com.neverwinterdp.scribengin.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.neverwinterdp.scribengin.clusterBuilder.SupportClusterBuilder;

public class ScribeConsumerClusterTestHelper {
  public ScribeConsumerClusterTestHelper() {}

  private String TOPIC = "cluster.test";
  private int numOfMessages = 100;
  private SupportClusterBuilder supportClusterBuilder;


  public String getTopic() {
    return TOPIC;
  }

  public String getHadoopConnection() {
    return supportClusterBuilder.getHadoopConnection();
  }

  public void setup() throws Exception {
    supportClusterBuilder =
        new SupportClusterBuilder("0.8.1.1", "127.0.0.1", 2181, "127.0.0.1", 2192);
    supportClusterBuilder.install();
    Thread.sleep(2000);
  }

  public void teardown() throws Exception {
    try {
      supportClusterBuilder.uninstall();
    } catch (Exception e) {
    }
    Thread.sleep(2000);
  }

  public void createKafkaData(int startNum) {
    //Write numOfMessages to Kafka
    Properties producerProps = new Properties();
    producerProps.put("metadata.broker.list", "localhost:9092");
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("request.required.acks", "1");

    Producer<String, String> producer =
        new Producer<String, String>(new ProducerConfig(producerProps));
    for (int i = startNum; i < startNum + numOfMessages; i++) {
      KeyedMessage<String, String> data =
          new KeyedMessage<String, String>(TOPIC, "Neverwinter" + Integer.toString(i));
      producer.send(data);
    }
    producer.close();
  }


  /**
   * Read in file, return whole file as a string
   * @param hdfsPath Path of HDFS file to read
   * @return whole file as a string
   */
  public void assertHDFSmatchesKafka(int startNum, String hdfsPath) {
    //int count = 0;
    String tempLine = "";
    String readLine = "";
    try {
      FileSystem fs = FileSystem.get(URI.create(hdfsPath), new Configuration());
      Path directory = new Path("/committed");
      RemoteIterator<LocatedFileStatus> directoryIterator = fs.listFiles(directory, false);
      while (directoryIterator.hasNext()) {
        Path p = directoryIterator.next().getPath();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        while ((tempLine = br.readLine()) != null) {
          readLine += tempLine;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Could not read from HDFS", false);
    }

    //Build string that will match
    String assertionString = "";
    for (int i = 0; i < startNum + numOfMessages; i++) {
      assertionString += "Neverwinter" + Integer.toString(i);
    }
    assertEquals("Data passed into Kafka did not match what was read from HDFS", assertionString,
        readLine);
  }

}
