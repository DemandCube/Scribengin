package com.neverwinterdp.kafka.producer;

import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.tool.KafkaTopicReport;

public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private Stopwatch totalRunDuration = Stopwatch.createUnstarted();
  @Test
  public void testRunner() throws Exception {
    Parameters[] parameters = {
      new Parameters("default"/*writer*/, 1024/*messageSize*/, 3 /*partitions*/, 2/*replications*/, 10000/*max send*/, 180000/*maxDuration*/),
      new Parameters("default"/*writer*/, 1024/*messageSize*/, 3 /*partitions*/, 2/*replications*/, 100000/*max send*/, 180000/*maxDuration*/),
      new Parameters("ack"    /*writer*/, 1024/*messageSize*/, 3 /*partitions*/, 2/*replications*/, 100000/*max send*/, 180000/*maxDuration*/)
    };
    KafkaTopicReport[] topicReport =new KafkaTopicReport[parameters.length];
    totalRunDuration.start();
    for (int i = 0; i < parameters.length; i++) {
      AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(parameters[i].getParameters());
      runner.setUp();
      runner.run();
      runner.tearDown();
      topicReport[i] = runner.getKafkaTopicReport();
    }
    totalRunDuration.stop();
    KafkaTopicReport.report(System.out, topicReport);
  }
  
  static public class Parameters {
    private String writerType = "ack";
    private int    messageSize = 1024;
    private int    partitions = 3 ;
    private int    replications = 2;
    private int    maxDuration     = 120000;
    private int    maxSend      = 10000;
    
    
    Parameters(String writerType, int messageSize, int partitions, int replications, int maxSend, int maxDuration) {
      this.writerType = writerType;
      this.messageSize = messageSize;
      this.partitions = partitions;
      this.replications = replications;
      this.maxSend = maxSend;
      this.maxDuration = maxDuration;
    }
    
    public String[] getParameters() {
      String[] args = {
          "--topic", "hello",
          "--num-partition",  Integer.toString(partitions),
          "--replication", Integer.toString(replications),
          
          "--send-writer-type", writerType,
          "--send-period", "0",
          "--send-message-size",  Integer.toString(messageSize),
          "--send-max-per-partition", Integer.toString(maxSend),
          "--send-max-duration", Integer.toString(maxDuration),
          
          "--producer:message.send.max.retries=5",
          "--producer:retry.backoff.ms=100",
          
          "--producer:queue.buffering.max.ms=1000",
          "--producer:queue.buffering.max.messages=15000",
          
          "--producer:topic.metadata.refresh.interval.ms=-1",
          "--producer:batch.num.messages=100",
          "--producer:acks=all",
          
          "--consume-max-duration", Integer.toString(maxDuration)
        };
      return args ;
    }
  }
}
