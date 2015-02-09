package com.neverwinterdp.scribengin.dataflow.test;

import java.util.Random;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.kafka.KafkaClient;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class HelloKafkaDataflowBuilder {
  private String     name  = "hello";
  private String     zkConnect              = "127.0.0.1:2181";
  private String     topic                  = "hello";

  private int numOfWorkers = 3;
  private int numOfExecutorPerWorker = 3;
  
  private DataflowClient dataflowClient;
  
  public HelloKafkaDataflowBuilder(ScribenginClient scribenginClient) {
    dataflowClient = new DataflowClient(scribenginClient);
  }

  
  public void setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
  }

  public void setNumOfExecutorPerWorker(int numOfExecutorPerWorker) {
    this.numOfExecutorPerWorker = numOfExecutorPerWorker;
  }

  public ScribenginWaitingEventListener submit() throws Exception {
    KafkaClient client = new KafkaClient(name, zkConnect) ;
    client.connect();
    String brokerList = client.getKafkaBrokerList() ;
    client.close();

    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setDataProcessor(TestCopyDataProcessor.class.getName());
    
    SourceDescriptor sourceDescriptor = new SourceDescriptor("KAFKA") ;
    sourceDescriptor.attribute("name", name);
    sourceDescriptor.attribute("topic", topic + ".source");
    sourceDescriptor.attribute("zk.connect", zkConnect);
    sourceDescriptor.attribute("broker.list", brokerList);
    dflDescriptor.setSourceDescriptor(sourceDescriptor);
    
    SinkDescriptor defaultSink = new SinkDescriptor("KAFKA");
    defaultSink.attribute("name", name);
    defaultSink.attribute("topic", topic + ".sink.default");
    defaultSink.attribute("zk.connect", zkConnect);
    defaultSink.attribute("broker.list", brokerList);
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    
    SinkDescriptor invalidSink = new SinkDescriptor("KAFKA");
    invalidSink.attribute("name", name);
    invalidSink.attribute("topic", topic + ".sink.invalid");
    invalidSink.attribute("zk.connect", zkConnect);
    invalidSink.attribute("broker.list", brokerList);
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    return dataflowClient.submit(dflDescriptor);
  }
  
  
  static public class TestCopyDataProcessor implements DataProcessor {
    private int count = 0;
    private Random random = new Random();
    
    @Override
    public void process(Record record, DataflowTaskContext ctx) throws Exception {
      if(random.nextDouble() < 0.8) {
        ctx.write(record);
        //System.out.println("Write default");
      } else {
        ctx.write("invalid", record);
        //System.out.println("Write invalid");
      }
      count++ ;
      if(count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }
}
