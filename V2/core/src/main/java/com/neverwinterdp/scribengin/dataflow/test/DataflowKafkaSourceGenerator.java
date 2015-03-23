package com.neverwinterdp.scribengin.dataflow.test;

import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.kafka.tool.KafkaMessageGenerator;
import com.neverwinterdp.kafka.tool.KafkaMessageSendTool;
import com.neverwinterdp.kafka.tool.KafkaTopicReport;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSourceGeneratorReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.util.JSONSerializer;

public class DataflowKafkaSourceGenerator extends DataflowSourceGenerator {
  private KafkaMessageSendTool sendTool;
  
  private String zkConnect ;
  
  @Override
  public void init(ScribenginClient scribenginClient) {
    zkConnect = scribenginClient.getRegistry().getRegistryConfig().getConnect();
    String[] sendArgs = {
        "--topic",                  sourceName, 
        "--num-partition",          Integer.toString(numberOfStream),
        "--send-period",            Long.toString(writePeriod),
        "--send-max-per-partition", Integer.toString(maxRecordsPerStream),
        "--send-max-duration",      Long.toString(maxDuration),
        "--zk-connect",             zkConnect
    };
    sendTool = new KafkaMessageSendTool();
    new JCommander(sendTool, sendArgs);
    sendTool.setMessageGenerator(new KafkaMessageGeneratorRecord());
  }

  
  @Override
  public void run() {
    sendTool.run();
  }

  @Override
  public void runInBackground() {
    sendTool.runAsDeamon();
  }
  
  @Override
  public void populate(DataflowTestReport report) {
    KafkaTopicReport topicReport = sendTool.getReport();
    DataflowSourceGeneratorReport sourceReport = report.getSourceGeneratorReport() ;
    sourceReport.setSourceName(topicReport.getTopic());
    sourceReport.setNumberOfStreams(topicReport.getNumOfPartitions());
    sourceReport.setWriteCount(topicReport.getProducerReport().getMessageSent());
    sourceReport.setDuration(topicReport.getProducerReport().getRunDuration());
  }
  
  @Override
  public StorageDescriptor getSourceDescriptor() {
    StorageDescriptor sourceDescriptor = new StorageDescriptor("kafka") ;
    sourceDescriptor.attribute("name", "DataflowKafkaSourceGenerator");
    sourceDescriptor.attribute("topic", sourceName);
    sourceDescriptor.attribute("zk.connect", zkConnect);
    return sourceDescriptor;
  }
  
  static public class KafkaMessageGeneratorRecord implements KafkaMessageGenerator {
    static public AtomicLong idTracker = new AtomicLong() ;
    
    public byte[] nextMessage(int partition, int messageSize) {
      String key = "partition=" + partition + ",id=" + idTracker.getAndIncrement();
      return JSONSerializer.INSTANCE.toString(new Record(key, new byte[messageSize] )).getBytes();
    }
  }
}
