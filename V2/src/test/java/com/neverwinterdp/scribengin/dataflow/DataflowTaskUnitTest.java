package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.hdfs.DataGenerator;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.hdfs.sink.SinkImpl;
import com.neverwinterdp.scribengin.hdfs.source.SourceImpl;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.util.FileUtil;

public class DataflowTaskUnitTest {
  static String      SOURCE_DIRECTORY       = "./build/hdfs/source";
  static String      SINK_DIRECTORY         = "./build/hdfs/sink";
  static String      INVALID_SINK_DIRECTORY = "./build/hdfs/invalid-sink";

  private FileSystem fs ;
  private DataflowContainer dataflowContainer;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    
    dataflowContainer = new DataflowContainer(new HashMap<String, String>());

    fs = FileSystem.getLocal(new Configuration()) ;
    Sink sink = new SinkImpl(fs, SOURCE_DIRECTORY);
    
    for(int i = 0; i < 3; i++) {
      DataGenerator.generateNewStream(sink, 3, 5);
    }
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void test() throws Exception {
    Source source = new SourceImpl(fs, SOURCE_DIRECTORY) ;
    Sink sink = new SinkImpl(fs, SINK_DIRECTORY);
    Sink invalidSink = new SinkImpl(fs, INVALID_SINK_DIRECTORY);
    
    SourceStream[] sourceStream = source.getStreams();
    DataflowTaskRunner[] runner = new DataflowTaskRunner[sourceStream.length]; 
    ExecutorService service = Executors.newFixedThreadPool(runner.length);
    for(int i = 0; i < sourceStream.length; i++) {
      DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
      descriptor.setId(i);
      descriptor.setDataProcessor(TestCopyDataProcessor.class.getName());
      descriptor.setSourceStreamDescriptor(sourceStream[i].getDescriptor());
      descriptor.add("default", sink.newStream().getDescriptor());
      descriptor.add("invalid", invalidSink.newStream().getDescriptor());
      
      //The Descriptor suppose to push to the registry and wait for a Task worker executor to pickup and run
      runner[i] = new DataflowTaskRunner(descriptor);
      service.execute(runner[i]);
    }
    service.shutdown();
    service.awaitTermination(10000, TimeUnit.MILLISECONDS);
    
    HDFSUtil.dump(fs, SINK_DIRECTORY);
    HDFSUtil.dump(fs, INVALID_SINK_DIRECTORY);
  }
  
  public class DataflowTaskRunner implements Runnable {
    DataflowTaskDescriptor descriptor ;
    
    public DataflowTaskRunner(DataflowTaskDescriptor descriptor) {
      this.descriptor = descriptor;
    }
    
    @Override
    public void run() {
      try {
        DataflowTask task = new DataflowTask(dataflowContainer, descriptor);
        task.execute();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  static public class TestCopyDataProcessor implements DataProcessor {
    private int count = 0;
    private Random random = new Random();
    
    @Override
    public void process(Record record, DataflowTaskContext ctx) throws Exception {
      if(random.nextDouble() < 0.8) ctx.write(record);
      else ctx.write("invalid", record);
      count++ ;
      if(count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }

}