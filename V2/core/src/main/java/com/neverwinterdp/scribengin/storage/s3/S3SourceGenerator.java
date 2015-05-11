package com.neverwinterdp.scribengin.storage.s3;

import com.google.common.base.Stopwatch;
import com.neverwinterdp.scribengin.dataflow.test.DataflowSourceGenerator.RecordMessageGenerator;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3SourceGenerator {

  private RecordMessageGenerator recordGenerator = new RecordMessageGenerator();

  private int numOfBufferPerStream;
  private int numOfRecordPerBuffer;

  private Stopwatch stopwatch = Stopwatch.createUnstarted();

  public void generateSource(S3Client s3Client, String bucketName, String folderPath, int numBufferPerStream, int numRecordsPerBuffer) throws Exception {
    stopwatch.start();
    System.out.println("generating test Data...");
    this.numOfBufferPerStream = numBufferPerStream;
    this.numOfRecordPerBuffer = numRecordsPerBuffer;

    StorageDescriptor descriptor = new StorageDescriptor("s3", bucketName);
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderPath);

    Sink sink = new S3Sink(s3Client, descriptor);
    generateStream(sink);
 
    System.out.println("---Finished generating test data in ---> " + stopwatch.stop());
    S3Util.listObjects(s3Client, bucketName);
  }

  void generateStream(Sink sink) throws Exception {
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for (int i = 0; i < numOfBufferPerStream; i++) {
      for (int j = 0; j < numOfRecordPerBuffer; j++) {
        writer.append(recordGenerator.nextRecord(i, 2));
      }
      writer.commit();
    }
    writer.close();
  }
}
