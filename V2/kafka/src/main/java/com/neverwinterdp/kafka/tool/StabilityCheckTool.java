package com.neverwinterdp.kafka.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.util.text.TabularFormater;

/**
 * The goal of the tool is to test the stability of kafka with random network, broker, disk failure.
 * The tool should:
 * 1. Create a number of writer equals to the number of partition, write a message with the write period. The writer
 *    should stop when either max-message-per-partition or max-duration reach first.
 * 2. Create a number of the partitioner reader equals to the number of partition, consume the message and verify that 
 *    the number of message equals to the number of sent message in writer
 * 3. Exit the tool, when either the readers consume all the messages or the exit-wait-time expires. The exit-wait-time
 *    start when all the writers terminate.
 * @author Tuan
 */
public class StabilityCheckTool {
  final static public String NAME = "StabilityCheckTool";
  
  @Parameter(names = "--zk-connect", description = "The zk connect string")
  private String zkConnect = "127.0.0.1:2181";
  
  @Parameter(names = "--topic", description = "The topic to test")
  private String topic = "hello";
  
  @Parameter(names = "--num-partition", description = "Number of the partitions")
  private int    numberOfPartition = 5;
  
  @Parameter(names = "--write-period", description = "Write period in ms per partition")
  private long   writePeriod = 100;
  
  @Parameter(names = "--max-duration", description = "The max duration in ms")
  private long   maxDuration = 10000;
  
  @Parameter(names = "--max-message-per-partition", description = "The max number of message per partition")
  private int    maxMessagePerPartition = 1000;
  
  @Parameter(names = "--message-size", description = "The message size in bytes")
  private int    messageSize = 100;
  
  @Parameter(names = "--replication", description = "The number of the replication")
  private int    replication = 1;
  
  @Parameter(names = "--exit-wait-time", description = "The message size in bytes")
  private long    exitWaitTime = 10000;
  
  private Map<String, String> kafkaProducerProps = new HashMap<String, String>();
  
  private String sampleData ;
  
  public void run(String[] args) throws Exception {
    new JCommander(this, args);
    run();
  }
  
  public void run() throws Exception {
    byte[] sampleDataBytes = new byte[messageSize];
    sampleData = new String(sampleDataBytes);
    
    Map<Integer, PartitionMessageWriter> writers = new HashMap<Integer, PartitionMessageWriter>();
    ExecutorService writerService = Executors.newFixedThreadPool(numberOfPartition);
    
    Map<Integer, PartitionMessageReader> readers = new HashMap<Integer, PartitionMessageReader>();
    ExecutorService readerService = Executors.newFixedThreadPool(numberOfPartition);
    
    KafkaTool kafkaTool = new KafkaTool(NAME, zkConnect);
    kafkaTool.connect();
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    kafkaTool.createTopic(topic, replication, numberOfPartition);
    TopicMetadata topicMetadata = kafkaTool.findTopicMetadata(topic);
    List<PartitionMetadata> partitionMetadataHolder = topicMetadata.partitionsMetadata();
    for(PartitionMetadata sel : partitionMetadataHolder) {
      PartitionMessageWriter writer = new PartitionMessageWriter(sel, kafkaConnects);
      writers.put(sel.partitionId(), writer);
      writerService.submit(writer);
      
      PartitionMessageReader reader = new PartitionMessageReader(sel);
      readers.put(sel.partitionId(), reader);
      readerService.submit(reader);
    }
    writerService.shutdown();
    readerService.shutdown();
    
    writerService.awaitTermination(maxDuration, TimeUnit.MILLISECONDS);
    
    readerService.awaitTermination(exitWaitTime, TimeUnit.MILLISECONDS);
    
    TabularFormater formater = new TabularFormater("Partition", "Write", "Read");
    formater.setIndent("  ");
    for (PartitionMetadata sel : partitionMetadataHolder) {
      int partitionId = sel.partitionId();
      PartitionMessageWriter writer = writers.get(partitionId);
      PartitionMessageReader reader = readers.get(partitionId);
      formater.addRow(sel.partitionId(), writer.writeCount, reader.readCount);
    }
    
    System.out.println(formater.getFormatText());
    kafkaTool.close();
  }
  
  public class PartitionMessageWriter implements Runnable {
    private PartitionMetadata metadata;
    private String kafkaConnects ;
    private int writeCount = 0;
    
    PartitionMessageWriter(PartitionMetadata metadata, String kafkaConnects) {
      this.metadata = metadata;
      this.kafkaConnects = kafkaConnects;
    }
    
    public void run() {
      KafkaWriter writer = new KafkaWriter(NAME, kafkaProducerProps, kafkaConnects);
      try {
        boolean terminated = false ;
        while(!terminated) {
          String key = "p:" + metadata.partitionId() + ":" + writeCount ;
          writer.send(topic, key, sampleData);
          writeCount++;
          //Check max message per partition
          if(writeCount >= maxMessagePerPartition) {
            terminated = true;
          } else if(writePeriod > 0) {
            Thread.sleep(writePeriod);
          }
        }
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if(writer != null) {
          writer.close();
        }
      }
    }
  }
  
  public class PartitionMessageReader implements Runnable {
    private PartitionMetadata metadata;
    private int readCount = 0;
    
    PartitionMessageReader(PartitionMetadata metadata) {
      this.metadata = metadata;
    }
    
    public void run() {
      KafkaPartitionReader partitionReader = new KafkaPartitionReader(NAME, topic, metadata);
      try {
        while(readCount < maxMessagePerPartition) {
          List<byte[]> messages = partitionReader.fetch(3000, 100);
          for(int i = 0; i < messages.size(); i++) {
            byte[] message = messages.get(i) ;
            readCount++;
          }
        }
      } catch(InterruptedException ex) {
      } catch(Exception ex) {
        ex.printStackTrace();
      } finally {
        try {
          partitionReader.commit();
          partitionReader.close();
        } catch(Exception ex) {
          ex.printStackTrace();
        }
      }
    }
  }
  
  static public void main(String[] args) throws Exception {
    StabilityCheckTool tool = new StabilityCheckTool();
    tool.run(args);
  }
}
