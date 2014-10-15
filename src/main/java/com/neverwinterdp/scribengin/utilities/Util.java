package com.neverwinterdp.scribengin.utilities;

import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;


public class Util {

  private static final Logger logger = Logger.getLogger(Util.class);

  public static LocalResource newYarnAppResource(
      FileSystem fs, Path path,
      LocalResourceType type, LocalResourceVisibility vis) throws IOException {
    Path qualified = fs.makeQualified(path);
    FileStatus status = fs.getFileStatus(qualified);
    LocalResource resource = Records.newRecord(LocalResource.class);
    resource.setType(type);
    resource.setVisibility(vis);
    resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified));
    resource.setTimestamp(status.getModificationTime());
    resource.setSize(status.getLen());
    return resource;
  }

  public static boolean isOpen(int port) {
    try (Socket ignored = new Socket("127.0.0.1", port)) {
      return true;
    } catch (IOException ignored) {
      return false;
    }
  }

  public static void createKafkaData(String kafkaHost, int kafkaPort, String topic) {
    logger.info("createKafkaData. ");
    Random rnd = new Random();
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaHost + ":" + kafkaPort);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "0");

    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);
    Date date = new Date();
    for (long nEvents = 0; nEvents < 999; nEvents++) {
      long runtime = date.getTime();
      String ip = "192.168.2." + rnd.nextInt(255);
      String msg = runtime + ",www.example.com," + ip;
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
      producer.send(data);
    }
    producer.close();
  }
}
