package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.test.HelloKafkaDataflowBuilder.TestCopyScribe;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.kafka.KafkaSourceGenerator;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class KafkaDataflowTest extends DataflowTest {
  final static public String SOURCE_TOPIC       = "hello.source" ;
  final static public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  final static public String INVALID_SINK_TOPIC = "hello.sink.invalid" ;
  
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

    KafkaSourceGenerator generator = new KafkaSourceGenerator("hello", zkConnect);
    generator.setNumOfPartitions(numPartitions);
    generator.setDuration(duration);
    generator.setWritePeriod(writePeriod);
    generator.setMaxNumOfRecordPerStream(maxMessagePerPartition);
    generator.generate(SOURCE_TOPIC);

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
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
  }
}