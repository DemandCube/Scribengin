package com.neverwinterdp.kafka.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.producer.KafkaWriter;

/**
 * The goal of the tool is to test the stability of kafka with random network, broker, disk failure.
 * The tool should:
 * 1. Create a number of writer equals to the number of partition, write a message with the write period. The writer
 *    should stop when either max-message-per-partition or max-duration reach first.
 * 2. Create a number of the partitioner reader equals to the number of partition, consume the message and verify that 
 *    the number of message equals to the number of sent message in writer
 * 3. Exit the tool, when either the readers consume all the messages or the exit-wait-time expires. The exit-wait-time
 *    start when all the writers terminate.
 *    
 * @author Tuan
 */
public class StabilityTool {
  final static public String NAME = "StabilityTool";
  
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
  
  @Parameter(names = "--exit-wait-time", description = "The message size in bytes")
  private long    exitWaitTime = 10000;
  
  
  public void run(String[] args) throws Exception {
    new JCommander(this, args);
    run();
  }
  
  public void run() throws Exception {
    Map<Integer, PartitionMessageWriter> writers = new HashMap<Integer, PartitionMessageWriter>();
    Map<Integer, PartitionMessageReader> readers = new HashMap<Integer, PartitionMessageReader>();
    
    Map<String, String> kafkaProducerProps = new HashMap<String, String>();
    kafkaProducerProps.put("partitioner.class", KeyPartitioner.class.getName());
    
    KafkaTool kafkaTool = new KafkaTool(NAME, zkConnect);
    kafkaTool.connect();
    TopicMetadata topicMetadata = kafkaTool.findTopicMetadata(topic);
    List<PartitionMetadata> partitionMetadataHolder = topicMetadata.partitionsMetadata();
    for(PartitionMetadata sel : partitionMetadataHolder) {
      
    }
  }
  
  public class PartitionMessageWriter extends Thread {
    private PartitionMetadata metadata;
    private int writeCount = 0;
    
    PartitionMessageWriter(PartitionMetadata metadata) {
      this.metadata = metadata;
    }
    
    public void run() {
      KafkaWriter writer = new KafkaWriter(NAME, zkConnect);
      
    }
  }
  
  public class PartitionMessageReader extends Thread {
    private PartitionMetadata metadata;
    private int readCount = 0;
    
    PartitionMessageReader(PartitionMetadata metadata) {
      this.metadata = metadata;
    }
    
    public void run() {
      
    }
  }
  
  public class KeyPartitioner implements Partitioner {
    public KeyPartitioner(VerifiableProperties props) {}

    public int partition(Object key, int numPartitions) {
      String keyStr = (String) key;
      int partition = Math.abs(keyStr.hashCode() % numPartitions);
      return partition;
    }
  }
}
