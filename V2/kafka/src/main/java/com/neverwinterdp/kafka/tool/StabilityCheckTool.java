package com.neverwinterdp.kafka.tool;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool.MessageCounter;
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
  
  @Parameter(names = "--send-timeout", description = "Timeout when the writer cannot send due to error or the buffer is full")
  private long    sendTimeout = 10000;
  
  @Parameter(names = "--tap-enable", description = "If set, outputs TAP")
  private boolean    tapEnabled = false;
  
  @Parameter(names = "--tap-file", description = "If TAP is enabled, then output results to this file.")
  private String tapFile = "stabilitychecktool.xml";
  
  
  private Map<String, String> kafkaProducerProps = new HashMap<String, String>();
  
  private String sampleData ;
  
  public void run(String[] args) throws Exception {
    JCommander jcommander = new JCommander(this, args);
    jcommander.usage();
    run();
  }
  
  public void run() throws Exception {
    TapProducer tapProducer = null;
    TestSet testSet =null;
    int testNum = 0;
    if(tapEnabled){
      tapProducer = TapProducerFactory.makeTapJunitProducer(tapFile);
      testSet = new TestSet();
    }
    
    byte[] sampleDataBytes = new byte[messageSize];
    sampleData = new String(sampleDataBytes);
    
    Map<Integer, PartitionMessageWriter> writers = new HashMap<Integer, PartitionMessageWriter>();
    ExecutorService writerService = Executors.newFixedThreadPool(numberOfPartition);
    
    KafkaMessageCheckTool messageCheckTool = new KafkaMessageCheckTool(zkConnect, topic, numberOfPartition * maxMessagePerPartition);
    
    KafkaTool kafkaTool = new KafkaTool(NAME, zkConnect);
    kafkaTool.connect();
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    if(kafkaTool.topicExits(topic)) {
      kafkaTool.deleteTopic(topic);
    }
    kafkaTool.createTopic(topic, replication, numberOfPartition);
    TopicMetadata topicMetadata = kafkaTool.findTopicMetadata(topic);
    List<PartitionMetadata> partitionMetadataHolder = topicMetadata.partitionsMetadata();
    for(PartitionMetadata sel : partitionMetadataHolder) {
      PartitionMessageWriter writer = new PartitionMessageWriter(sel, kafkaConnects);
      writers.put(sel.partitionId(), writer);
      writerService.submit(writer);
    }
    messageCheckTool.runAsDeamon();
    writerService.shutdown();
    
    writerService.awaitTermination(maxDuration, TimeUnit.MILLISECONDS);
    if(!writerService.isTerminated()) {
      writerService.shutdownNow();
    }
    messageCheckTool.waitForTermination(maxDuration);
    
    TabularFormater formater = new TabularFormater("Partition", "Write", "Read");
    MessageCounter messageCounter = messageCheckTool.getMessageCounter();
    formater.setIndent("  ");
    for (PartitionMetadata sel : partitionMetadataHolder) {
      int partitionId = sel.partitionId();
      PartitionMessageWriter writer = writers.get(partitionId);
      formater.addRow(sel.partitionId(), writer.writeCount, messageCounter.getPartitionCount(partitionId));
      
      //Deal with TAP output
      if(tapEnabled){
        TestResult t = null;
        if(writer.writeCount == messageCounter.getPartitionCount(partitionId)){
          t = new TestResult( StatusValues.OK, ++testNum );
        }
        else{
          t = new TestResult( StatusValues.NOT_OK, ++testNum );
        }
        
        t.setDescription( "Test if messages written == messages count from partition "+Integer.toString(sel.partitionId()) );
        testSet.addTestResult( t );
      }
    }
    
    System.out.println(formater.getFormatText());
    kafkaTool.close();
    
    if(tapEnabled){
      //System.err.println(tapProducer.dump(testSet));
      tapProducer.dump(testSet, new File(tapFile));
    }
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
      DefaultKafkaWriter writer = new DefaultKafkaWriter(NAME, kafkaProducerProps, kafkaConnects);
      try {
        boolean terminated = false ;
        while(!terminated) {
          String key = "p:" + metadata.partitionId() + ":" + writeCount ;
          writer.send(topic,  metadata.partitionId(), key, sampleData, sendTimeout);
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
  
  static public void main(String[] args) throws Exception {
    StabilityCheckTool tool = new StabilityCheckTool();
    tool.run(args);
  }
}
