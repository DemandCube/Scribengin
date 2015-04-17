package com.neverwinterdp.scribengin.dataflow.test;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Stopwatch;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSourceGeneratorReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class HDFSDataflowSourceGenerator extends DataflowSourceGenerator {

  private RecordMessageGenerator recordGenerator = new RecordMessageGenerator() ;
  private Stopwatch stopwatch = Stopwatch.createUnstarted();
  private FileSystem fs  ;
  
  private int numOfFilesPerFolder;
  private int numOfRecordsPerFile;

  @Override
  public StorageDescriptor getSourceDescriptor() {
    String location = sourceLocation + "/" + sourceName ;
    StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", location) ;
    return storageDescriptor;
  }

  @Override
  public void init(ScribenginClient scribenginClient) throws Exception {
    fs = scribenginClient.getVMClient().getFileSystem();
    numOfFilesPerFolder=1;
    numOfRecordsPerFile = maxRecordsPerStream/numOfFilesPerFolder;
    }

  @Override
  public void run() {
    stopwatch.start();
    try {
      String location = sourceLocation + "/" + sourceName;
      generateSource(fs, location);
      stopwatch.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void runInBackground() {
    throw new RuntimeException("this method is not supported for the hdfs due to the nature of the storage") ;
  }

  @Override
  public void populate(DataflowTestReport report) {
    DataflowSourceGeneratorReport sourceReport = report.getSourceGeneratorReport() ;
    sourceReport.setSourceName(sourceName);
    sourceReport.setNumberOfStreams(numberOfStream);
    sourceReport.setWriteCount(RecordMessageGenerator.idTracker.get());
    sourceReport.setDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }
  
  void generateSource(FileSystem fs, String sourceDir) throws Exception {
    SinkFactory sinkFactory = new SinkFactory(fs);
    StorageDescriptor sinkDescriptor = new StorageDescriptor("hdfs", sourceDir);
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int i = 0; i < numberOfStream; i++) {
      generateStream(sink);
    }
  }

  void generateStream(Sink sink) throws Exception {
    SinkStream stream = sink.newStream();
    int partition = stream.getDescriptor().getId() ;
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < numOfFilesPerFolder; i++) {
      for(int j = 0; j < numOfRecordsPerFile; j ++) {
        writer.append(recordGenerator.nextRecord(partition, recordSize));
      }
      writer.commit();
    }
    writer.close();
  }
}