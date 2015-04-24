package com.neverwinterdp.scribengin.dataflow.test;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSinkValidatorReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.tool.message.MessageExtractor;
import com.neverwinterdp.tool.message.MessageTracker;

public class S3DataflowSinkValidator extends DataflowSinkValidator {
  private S3Client s3Client;
  private MessageTracker messageTracker;
  private Stopwatch stopwatch = Stopwatch.createUnstarted();

  @Override
  public StorageDescriptor getSinkDescriptor() {
    System.err.println("location " + sinkLocation);
    System.err.println("sinkName " + sinkName);
    //bucket is sink name
    StorageDescriptor storageDescriptor = new StorageDescriptor("s3", sinkName);
    storageDescriptor.attribute("s3.bucket.name", sinkName);
    storageDescriptor.attribute("s3.storage.path", sinkLocation);
    storageDescriptor.attribute("s3.region.name", "eu-central-1");
    return storageDescriptor;
  }

  @Override
  public void init(ScribenginClient scribenginClient) throws Exception {
    s3Client = new S3Client();
    s3Client.onInit();
   // scribenginClient.getVMClient().getS3Client();
  }

  @Override
  public void run() {
    stopwatch.start();
    messageTracker = new MessageTracker();
    MessageExtractor messageExtractor = MessageExtractor.DEFAULT_MESSAGE_EXTRACTOR;
    try {
      S3Source source = new S3Source(s3Client, getSinkDescriptor());
      SourceStream[] streams = source.getStreams();
      System.err.println("ngapi "+ streams.length);
      for (SourceStream selStream : streams) {
        SourceStreamReader streamReader = selStream.getReader("S3DataflowSinkValidator");
        Record record = null;
        while ((record = streamReader.next()) != null) {
          System.err.println("Record is not null "+ record);
          Message message = messageExtractor.extract(record.getData());
          messageTracker.log(message);
        }
        streamReader.close();
      }
      messageTracker.optimize();
      messageTracker.dump(System.out);
    } catch (Exception e) {
      e.printStackTrace();
    }
    stopwatch.stop();
  }

  @Override
  public void runInBackground() {
    throw new RuntimeException("Due to the nature of s3 storage, this method is not avaialable");
  }

  @Override
  public boolean waitForTermination() throws InterruptedException {
    throw new RuntimeException("Due to the nature of s3 storage, this method is not avaialable");
  }

  @Override
  public boolean waitForTermination(long timeout) throws InterruptedException {
    throw new RuntimeException("Due to the nature of s3 storage, this method is not avaialable");
  }

  @Override
  public void populate(DataflowTestReport report) {
    DataflowSinkValidatorReport sinkReport = report.getSinkValidatorReport();
    sinkReport.setSinkName(sinkName);
    sinkReport.setNumberOfStreams(messageTracker.getPartitionMessageTrackers().size());
    sinkReport.setReadCount(messageTracker.getLogCount());
    sinkReport.setDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }
}
