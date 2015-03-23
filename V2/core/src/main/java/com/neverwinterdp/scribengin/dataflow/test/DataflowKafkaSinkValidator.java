package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTopicReport;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSinkValidatorReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public class DataflowKafkaSinkValidator extends DataflowSinkValidator {
  private KafkaMessageCheckTool kafkaMessageCheckTool;
  private String zkConnect ;
  
  @Override
  public StorageDescriptor getSinkDescriptor() {
    StorageDescriptor sink = new StorageDescriptor("KAFKA");
    sink.attribute("name", "DataflowKafkaSinkValidator");
    sink.attribute("topic", sinkName);
    sink.attribute("zk.connect", zkConnect);
    return sink;
  }

  @Override
  public void init(ScribenginClient scribenginClient) {
    zkConnect = scribenginClient.getRegistry().getRegistryConfig().getConnect();
    String[] args = {
      "--topic",                  sinkName, 
      "--num-partition",          Integer.toString(5),
      "--zk-connect",             zkConnect
    };
    kafkaMessageCheckTool = new KafkaMessageCheckTool();
    new JCommander(kafkaMessageCheckTool, args);
  }
  
  @Override
  public void run() {
    kafkaMessageCheckTool.run();
  }

  @Override
  public void runInBackground() {
    kafkaMessageCheckTool.runAsDeamon();
  }

  @Override
  public boolean waitForTermination() throws InterruptedException {
    return kafkaMessageCheckTool.waitForTermination();
  }

  @Override
  public boolean waitForTermination(long timeout) throws InterruptedException {
    return kafkaMessageCheckTool.waitForTermination(timeout);
  }
  
  @Override
  public void populate(DataflowTestReport report) {
    KafkaTopicReport topicReport = kafkaMessageCheckTool.getReport() ;
    DataflowSinkValidatorReport sinkReport = report.getSinkValidatorReport();
    sinkReport.setSinkName(topicReport.getTopic());
    sinkReport.setNumberOfStreams(topicReport.getNumOfPartitions());
    sinkReport.setReadCount(topicReport.getConsumerReport().getMessagesRead());
    sinkReport.setDuration(topicReport.getConsumerReport().getRunDuration());
  }
}
