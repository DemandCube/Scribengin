package com.neverwinterdp.scribengin.dataflow.builder;

import java.util.Random;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.dataflow.DataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.kafka.KafkaClient;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class HelloKafkaDataflowBuilder extends DataflowBuilder {
  private String     name  = "hello";
  private String     zkConnect              = "127.0.0.1:2181";
  private String     topic                  = "hello";
  private String     brokerList             = "192.168.59.3:9092" ;

  private int numOfWorkers = 3;
  private int numOfExecutorPerWorker = 3;
  
  public HelloKafkaDataflowBuilder(ScribenginClusterBuilder clusterBuilder) {
    super(clusterBuilder);
  }

  
  public void setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
  }

  public void setNumOfExecutorPerWorker(int numOfExecutorPerWorker) {
    this.numOfExecutorPerWorker = numOfExecutorPerWorker;
  }


  public void createSource(int numOfStream, int numOfRecordPerStream) throws Exception {
    System.out.println("Start create data source for kafka");
    SinkFactory  sinkFactory = new SinkFactory(null);
    SinkDescriptor sinkDescriptor = new SinkDescriptor("kafka");
    sinkDescriptor.attribute("name", name);
    sinkDescriptor.attribute("topic", topic + ".source");
    sinkDescriptor.attribute("zk.connect", zkConnect);
    KafkaClient client = new KafkaClient(name, zkConnect) ;
    client.connect();
    sinkDescriptor.attribute("broker.list", client.getKafkaBrokerList());
    client.close();
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int k = 0; k < numOfStream; k++) {
      SinkStream stream = sink.newStream();
      SinkStreamWriter writer = stream.getWriter();
      for(int i = 0; i < numOfRecordPerStream; i++) {
        String hello = "Hello " + i ;
        Record record = new Record("key-" + i, hello.getBytes());
        writer.append(record);
      }
      writer.close();
    }
    sink.close();
    System.out.println("Finish create data source for kafka");
  }
  
  
  @Override
  protected DataflowDescriptor createDataflowDescriptor() {
    try {
      KafkaClient client = new KafkaClient(name, zkConnect) ;
      client.connect();
      brokerList = client.getKafkaBrokerList() ;
      client.close();
    } catch(Exception e) {
      e.printStackTrace();
    }
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("test-dataflow");
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
    
    return dflDescriptor;
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
