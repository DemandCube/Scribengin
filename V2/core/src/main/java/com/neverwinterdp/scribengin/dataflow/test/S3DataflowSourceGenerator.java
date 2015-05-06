package com.neverwinterdp.scribengin.dataflow.test;

import java.util.concurrent.TimeUnit;

import com.beust.jcommander.Parameter;
import com.google.common.base.Stopwatch;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSourceGeneratorReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3DataflowSourceGenerator extends DataflowSourceGenerator {

  @Parameter(names = "--sink-bucket-auto-create", description = "Auto create bucket if it doesn't exist")
  private boolean autoCreate = true;

  private RecordMessageGenerator recordGenerator = new RecordMessageGenerator();
  private Stopwatch stopwatch = Stopwatch.createUnstarted();
  private S3Client s3Client;

  private int numOfFilesPerFolder;
  private int numOfRecordsPerFile;

  @Override
  public StorageDescriptor getSourceDescriptor() {
    StorageDescriptor storageDescriptor = new StorageDescriptor("s3", sourceLocation);
    storageDescriptor.attribute("s3.bucket.name", sourceLocation);
    storageDescriptor.attribute("s3.storage.path", sourceName);
    storageDescriptor.attribute("s3.region.name", "eu-central-1");
    storageDescriptor.attribute("s3.bucket.autocreate", Boolean.toString(autoCreate));
    return storageDescriptor;
  }

  @Override
  public void init(ScribenginClient scribenginClient) throws Exception {
    s3Client = new S3Client();
    s3Client.onInit();
    numOfFilesPerFolder = 1;
    numOfRecordsPerFile = maxRecordsPerStream / numOfFilesPerFolder;
  }

  @Override
  public void run() {
    stopwatch.start();
    try {
      String location = sourceLocation + "/" + sourceName;
      generateSource(s3Client, location);
      stopwatch.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void runInBackground() {
    throw new RuntimeException("this method is not supported for the s3 due to the nature of the storage");
  }

  @Override
  public void populate(DataflowTestReport report) {
    DataflowSourceGeneratorReport sourceReport = report.getSourceGeneratorReport();
    sourceReport.setSourceName(sourceName);
    sourceReport.setNumberOfStreams(numberOfStream);
    sourceReport.setWriteCount(RecordMessageGenerator.idTracker.get());
    sourceReport.setDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  void generateSource(S3Client s3Client, String bucket) throws Exception {
    SinkFactory sinkFactory = new SinkFactory(s3Client);
    Sink sink = sinkFactory.create(getSourceDescriptor());

    for (int i = 0; i < numberOfStream; i++) {
      generateStream(sink);
    }
  }

  void generateStream(Sink sink) throws Exception {
    SinkStream stream = sink.newStream();
    int partition = stream.getDescriptor().getId();
    SinkStreamWriter writer = stream.getWriter();
    for (int i = 0; i < numOfFilesPerFolder; i++) {
      for (int j = 0; j < numOfRecordsPerFile; j++) {
        writer.append(recordGenerator.nextRecord(partition, recordSize));
      }
      writer.commit();
    }
    writer.close();
  }

  @Override
  public boolean canRunInBackground() {
    return false;
  }
}