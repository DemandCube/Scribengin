package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaMessageSendTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.kafka.tool.messagegenerator.KafkaMessageGeneratorRecord;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.test.HelloKafkaDataflowBuilder.TestCopyScribe;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;


public class KafkaDataflowTest extends DataflowTest {
  @Parameter(names = "--source-topic", description = "Source topic")
  public String SOURCE_TOPIC       = "hello.source" ;
  
  @Parameter(names = "--sink-topic", description = "Default sink topic")
  public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  
  @Parameter(names = "--invalidsink-topic", description = "Invalid sink topic")
  public String INVALID_SINK_TOPIC = "hello.sink.invalid" ;
  
  @Parameter(names = "--flow-name", description = "Invalid sink topic")
  private String name                   = "hello";
  
  @Parameter(names = "--kafka-write-period", description = "The write period for each partition in ms")
  private int writePeriod = 10;
  
  @Parameter(names = "--kafka-num-partition", description = "Number of the partitions")
  private int numPartitions = 5;
  
  @Parameter(names = "--kafka-max-message-per-partition", description = "Number of the partitions")
  private int maxMessagePerPartition = 100;
  
  protected void doRun(ScribenginShell shell) throws Exception {
    
    long start = System.currentTimeMillis();
    ScribenginClient scribenginClient = shell.getScribenginClient();
    RegistryConfig registryConfig = scribenginClient.getRegistry().getRegistryConfig();
    String zkConnect = registryConfig.getConnect();
    
    
    String[] sendArgs = {"--topic", SOURCE_TOPIC, 
                     "--send-period", Integer.toString(writePeriod),
                     "--num-partition", Integer.toString(numPartitions),
                     "--send-max-per-partition", Integer.toString(maxMessagePerPartition),
                     "--send-max-duration", Long.toString(duration),
                     "--zk-connect", zkConnect};
    KafkaMessageSendTool sendTool = new KafkaMessageSendTool();
    new JCommander(sendTool, sendArgs);
    sendTool.setMessageGenerator(new KafkaMessageGeneratorRecord());
    sendTool.runAsDeamon();

    KafkaTool client = new KafkaTool(name, zkConnect) ;
    client.connect();
    String brokerList = client.getKafkaBrokerList() ;
    client.close();

    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    SourceDescriptor sourceDescriptor = new SourceDescriptor("KAFKA") ;
    sourceDescriptor.attribute("name", name);
    sourceDescriptor.attribute("topic", SOURCE_TOPIC);
    sourceDescriptor.attribute("zk.connect", zkConnect);
    sourceDescriptor.attribute("broker.list", brokerList);
    dflDescriptor.setSourceDescriptor(sourceDescriptor);

    SinkDescriptor defaultSink = new SinkDescriptor("KAFKA");
    defaultSink.attribute("name", name);
    defaultSink.attribute("topic", DEFAULT_SINK_TOPIC);
    defaultSink.attribute("zk.connect", zkConnect);
    defaultSink.attribute("broker.list", brokerList);
    dflDescriptor.addSinkDescriptor("default", defaultSink);

    SinkDescriptor invalidSink = new SinkDescriptor("KAFKA");
    invalidSink.attribute("name", name);
    invalidSink.attribute("topic", INVALID_SINK_TOPIC);
    invalidSink.attribute("zk.connect", zkConnect);
    invalidSink.attribute("broker.list", brokerList);
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
    
    String[] checkArgs = {"--topic", DEFAULT_SINK_TOPIC,
        //"--num-partition", Integer.toString(numPartitions),
        "--consume-max-duration", Long.toString(duration),
        "--consume-max", Integer.toString(maxMessagePerPartition*numPartitions),
        "--zk-connect", zkConnect,
        "--tap-enable"};
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool();
    new JCommander(checkTool, checkArgs);
    checkTool.runAsDeamon();
    
    
    //TODO: Support making sure a topic stays empty
    /*
    String[] checkInvalidArgs = {"--topic", INVALID_SINK_TOPIC, 
        "--consume-max-duration", Integer.toString(writePeriod*this.numPartitions),
        "--consume-max", Integer.toString(maxMessagePerPartition*this.numPartitions),
        "--zk-connect", zkConnect};
    KafkaMessageCheckTool checkInvalidTool = new KafkaMessageCheckTool();
    new JCommander(checkInvalidTool, checkInvalidArgs);
    checkInvalidTool.setExpectNumberOfMessage(0);
    checkInvalidTool.runAsDeamon();
    */
    
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    checkTool.waitForTermination(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
  }
}