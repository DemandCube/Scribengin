package com.neverwinterdp.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;

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
public class KafkaStabilityCheckTool implements Runnable {
  @ParametersDelegate
  private KafkaConfig.Topic    topicConfig = new KafkaConfig.Topic();
  
  @ParametersDelegate
  private KafkaConfig.Producer senderConfig = new KafkaConfig.Producer();
  
  @ParametersDelegate
  private KafkaConfig.Consumer consumerConfig = new KafkaConfig.Consumer();
  
  
  private KafkaMessageSendTool  sendTool ;
  private KafkaMessageCheckTool checkTool ;
  private KafkaReport           report;
  
  public KafkaStabilityCheckTool(String[] args) throws Exception {
    JCommander jcommander = new JCommander(this, args);
    jcommander.usage();
    sendTool = new KafkaMessageSendTool(topicConfig, senderConfig);
    checkTool = new KafkaMessageCheckTool(topicConfig, consumerConfig);
  }
  
  public void run() {
    try {
      doRun() ;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void doRun() throws Exception {
    sendTool.runAsDeamon();
    Thread.sleep(2000);// wait to make sure send tool run
    checkTool.runAsDeamon();
    sendTool.waitForTermination();
    checkTool.waitForTermination(10000);
    System.out.println("Check Tool: " + checkTool.getMessageCounter().getTotal());
    
    report = new KafkaReport() ;
    //TODO: Update and poulate the report data
    sendTool.report(report);
    checkTool.report(report);
  }
  
  //TODO: create report object at the end of doRun
  public KafkaReport getKafkaReport() { return report; }
  
  public void report(Appendable out) {
  }
  
  static public void main(String[] args) throws Exception {
    KafkaStabilityCheckTool tool = new KafkaStabilityCheckTool(args);
    tool.run();
    //TODO: this method should print out the report in the table format
    tool.report(System.out);
  }
}
