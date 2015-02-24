package com.neverwinterdp.scribengin.dataflow;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSSourceGenerator;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HelloHDFSDataflowBuilder {
  private String dataDir ;
  private FileSystem fs ;
  private int numOfWorkers = 3;
  private int numOfExecutorPerWorker = 3;
  private ScribenginClient scribenginClient;
  
  public HelloHDFSDataflowBuilder(ScribenginClient scribenginClient, FileSystem fs, String dataDir) {
    this.scribenginClient = scribenginClient;
    this.fs = fs ;
    this.dataDir = dataDir ;
  }

  
  public void setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
  }

  public void setNumOfExecutorPerWorker(int numOfExecutorPerWorker) {
    this.numOfExecutorPerWorker = numOfExecutorPerWorker;
  }

  public ScribenginWaitingEventListener submit() throws Exception {
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-hdfs-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyDataProcessor.class.getName());
    SourceDescriptor sourceDescriptor = new SourceDescriptor("HDFS", dataDir + "/source") ;
    dflDescriptor.setSourceDescriptor(sourceDescriptor);
    SinkDescriptor defaultSink = new SinkDescriptor("HDFS", dataDir + "/sink");
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    SinkDescriptor invalidSink = new SinkDescriptor("HDFS", dataDir + "/invalid-sink");
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
    return scribenginClient.submit(dflDescriptor) ;
  }

  public void createSource(int numOfStream, int numOfBuffer, int numOfRecordPerBuffer) throws Exception {
    HDFSSourceGenerator generator = new HDFSSourceGenerator();
    generator.generateSource(fs, dataDir + "/source");
  }
}
