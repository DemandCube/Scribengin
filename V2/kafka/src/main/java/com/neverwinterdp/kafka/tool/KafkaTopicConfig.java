package com.neverwinterdp.kafka.tool;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class KafkaTopicConfig {
  @Parameter(names = "--zk-connect", description = "The zk connect string")
  public String zkConnect = "127.0.0.1:2181";
  
  @Parameter(names = "--topic", description = "The kafka topic to work in")
  public String topic = "hello";
  
  @Parameter(names = "--num-partition", description = "Number of the partition to create for the topic")
  public int    numberOfPartition = 5;

  @Parameter(names = "--replication", description = "The number of the replication for the topic")
  public int    replication = 1;
  
  
  @ParametersDelegate
  public Producer producerConfig = new Producer();
  
  @ParametersDelegate
  public Consumer consumerConfig = new Consumer();
  
  public KafkaTopicConfig() {} 
  
  public KafkaTopicConfig(String[] args) {
    new JCommander(this, args);
    consumerConfig.consumeMax = producerConfig.maxMessagePerPartition * numberOfPartition;
  }
  
  static public class Producer {
    
    @Parameter(names = "--send-writer-type", description = "The default producer writer or reliable producer writer(ack)")
    public String writerType = "default";
    
    @Parameter(names = "--send-period", description = "Write period in ms per partition")
    public long   sendPeriod = 100;
    
    @Parameter(names = "--send-max-duration", description = "The max send duration in ms")
    public long   maxDuration = 10000;
    
    @Parameter(names = "--send-max-per-partition", description = "The max number of message per partition")
    public int    maxMessagePerPartition = 1000;
    
    @Parameter(names = "--send-message-size", description = "The message size in bytes")
    public int    messageSize = 100;
    
    @Parameter(names = "--send-timeout", description = "Timeout when the writer cannot send due to error or the buffer is full")
    public long    sendTimeout = 10000;
    
    @DynamicParameter(names = "--producer:", description = "The kafka producer properties configuration according to the kafka producer document")
    public Map<String, String> producerProperties = new HashMap<String, String>();
  }
  
  static public class Consumer {
    @Parameter(names = "--consume-max-duration", description = "The max consume duration in ms")
    public long   maxDuration = 10000;
    
    @Parameter(names = "--consume-max", description = "The max number of messages to consume")
    public int   consumeMax = 100000000;
    
    @Parameter(names = "--consume-connect-retries", description = "The max number of times to try connecting while number of partitions is < 1")
    public int   connectRetries = 20;
    
    @Parameter(names = "--tap-enable", description = "If set, outputs TAP")
    public boolean    tapEnabled = false;
    
    @Parameter(names = "--tap-file", description = "If TAP is enabled, then output results to this file.")
    public String tapFile = "KafkaMessageCheckTool.xml";
    
    @DynamicParameter(names = "--consumer:", description = "The kafka consumer properties configuration according to the kafka consumer document")
    public Map<String, String> consumerProperties = new HashMap<String, String>();
  }
}
