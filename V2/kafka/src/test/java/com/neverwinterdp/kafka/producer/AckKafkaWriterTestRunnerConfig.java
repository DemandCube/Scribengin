package com.neverwinterdp.kafka.producer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class AckKafkaWriterTestRunnerConfig {
  @Parameter(names = "--topic", description = "The topic")
  private String topic = "hello";
  
  @Parameter(names = "--message-size", description = "The message size")
  private int messageSize = 4092;
  @Parameter(names = "--num-partition", description = "The number of partitions")
  private int numOfPartitions = 3;
  @Parameter(names = "--num-replication", description = "The number of replications")
  private int numOfReplications = 2;
  
  @Parameter(names = "--max-num-message", description = "The maximum number of the messages")
  private int maxNumOfMessages = 20000;
  
  @Parameter(names = "--num-kafka-brokers", description = "Number of kafka brokers.")
  private int numKafkaBrokers = 2;
  
  public AckKafkaWriterTestRunnerConfig(String[] args) {
    new JCommander(this, args);
  }
  
  public String getTopic() { return this.topic; }
  
  public int getMessageSize() { return messageSize; }
  
  public int getNumOfPartitions() { return numOfPartitions; }
  
  public int getNumOfReplications() { return numOfReplications; }
 
  public int getMaxNumOfMessages() { return maxNumOfMessages; }
  
  public int getNumKafkaBrokers() { return numKafkaBrokers; } 
}
