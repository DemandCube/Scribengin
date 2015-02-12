package com.neverwinterdp.scribengin.dataflow.test;

import java.util.Random;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.util.JSONSerializer;

public class HelloHDFSDataflowBuilder {
  private String dataDir ;
  private int numOfWorkers = 3;
  private int numOfExecutorPerWorker = 3;
  private DataflowClient dataflowClient ;
  private ScribenginClient scribenginClient;
  
  public HelloHDFSDataflowBuilder(ScribenginClient scribenginClient, String dataDir) {
    this.scribenginClient = scribenginClient;
    this.dataDir = dataDir ;
  }

  
  public void setNumOfWorkers(int numOfWorkers) { this.numOfWorkers = numOfWorkers; }

  public void setNumOfExecutorPerWorker(int numOfExecutorPerWorker) {
    this.numOfExecutorPerWorker = numOfExecutorPerWorker;
  }

  public ScribenginWaitingEventListener submit() throws Exception {
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-hdfs-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setDataProcessor(TestCopyDataProcessor.class.getName());
    SourceDescriptor sourceDescriptor = new SourceDescriptor("HDFS", dataDir + "/source") ;
    dflDescriptor.setSourceDescriptor(sourceDescriptor);
    SinkDescriptor defaultSink = new SinkDescriptor("HDFS", dataDir + "/sink");
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    SinkDescriptor invalidSink = new SinkDescriptor("HDFS", dataDir + "/invalid-sink");
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
    return scribenginClient.submit(dflDescriptor) ;
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
