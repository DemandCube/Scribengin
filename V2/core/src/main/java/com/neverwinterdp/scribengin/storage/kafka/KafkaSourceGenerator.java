package com.neverwinterdp.scribengin.storage.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.RecordChecksum;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

//TODO: remove and use the check tool
public class KafkaSourceGenerator {
  private String                        name;
  private String                        zkConnect               = "127.0.0.1:2181";
  private int                           numOfReplication        = 1;
  private int                           numPartitions           = 5;
  private int                           maxNumOfRecordPerStream = 100;
  private long                          duration                = 5 * 1000;
  private long                          writePeriod             = 0;

  private RecordChecksum checksum ;
  private Map<Integer, StreamGenerator> streamGenerators = new HashMap<Integer, StreamGenerator>();
  
  public KafkaSourceGenerator(String name, String zkConnect) throws Exception {
    this.name = name;
    this.zkConnect = zkConnect;
    checksum = new RecordChecksum();
  }
  
  public KafkaSourceGenerator setMaxNumOfRecordPerStream(int num) { 
    this.maxNumOfRecordPerStream = num ;
    return this;
  }
  
  public KafkaSourceGenerator setNumOfReplication(int num) { 
    this.numOfReplication = num ;
    return this;
  }
  
  public KafkaSourceGenerator setNumOfPartitions(int num) { 
    this.numPartitions = num ;
    return this;
  }
  
  public KafkaSourceGenerator setDuration(long num) { 
    this.duration = num ;
    return this;
  }
  
  public KafkaSourceGenerator setWritePeriod(long num) { 
    this.writePeriod = num ;
    return this;
  }
  
  public ExecutorService generate(String topic) throws Exception {
    createTopic(topic, numOfReplication, numPartitions);
    SinkFactory  sinkFactory = new SinkFactory(null);
    StorageDescriptor storageDescriptor = new StorageDescriptor("kafka");
    storageDescriptor.attribute("name", name);
    storageDescriptor.attribute("topic", topic);
    storageDescriptor.attribute("zk.connect", zkConnect);
    KafkaTool client = new KafkaTool(name, zkConnect) ;
    client.connect();
    storageDescriptor.attribute("broker.list", client.getKafkaBrokerList());
    client.close();
    Sink sink = sinkFactory.create(storageDescriptor);
    ExecutorService executorService = Executors.newFixedThreadPool(numPartitions);
    for(int k = 0; k < numPartitions; k++) {
      SinkStream stream = sink.newStream();
      StreamGenerator streamGenerator = new StreamGenerator(stream);
      streamGenerators.put(stream.getDescriptor().getId(), streamGenerator);
      executorService.submit(streamGenerator);
    }
    executorService.shutdown();
    return executorService;
  }
  
  public void generateAndWait(String topic) throws Exception {
    ExecutorService executorService = generate(topic);
    executorService.awaitTermination(duration, TimeUnit.MICROSECONDS);
  }

  synchronized void notifyTermination(StreamGenerator generator) {
    int id = generator.stream.getDescriptor().getId();
    streamGenerators.remove(id);
    System.out.println("Stream generator " + id + " is terminated, count = " + generator.count);
    notify() ;
  }
  
  public void createTopic(String topicName, int numOfReplication, int numPartitions) throws Exception {
    // Create a ZooKeeper client
    int sessionTimeoutMs = 1000;
    int connectionTimeoutMs = 1000;
    ZkClient zkClient = new ZkClient(zkConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    // Create a topic named "myTopic" with 8 partitions and a replication factor of 3
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkClient, topicName, numPartitions, numOfReplication, topicConfig);
    Thread.sleep(3000);
    zkClient.close();
  }
  
  public class StreamGenerator implements Runnable {
    SinkStream stream;
    int count ;
    
    public StreamGenerator(SinkStream stream) {
      this.stream = stream ;
    }
    
    @Override
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        SinkStreamWriter writer = stream.getWriter();
        for(int i = 0; i < maxNumOfRecordPerStream; i++) {
          String hello = "Hello " + i ;
          if(writePeriod > 0) Thread.sleep(writePeriod);
          Record record = new Record("key-" + i, hello.getBytes());
          writer.append(record);
          checksum.update(record);
          count++ ;
          if(writePeriod > 0) {
           long currentDuration =  System.currentTimeMillis() - startTime;
           if(currentDuration > duration) break ;
          }
        }
        writer.close();
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage()) ;
      } finally {
        notifyTermination(this);
      }
    }
  }
}