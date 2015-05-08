package com.neverwinterdp.scribengin.storage.s3.sink;

import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3ObjectWriter;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class S3SinkStreamWriter implements SinkStreamWriter {
  private S3Folder streamS3Folder;
  private String segmentName;
  private S3ObjectWriter writer;

  private final int TIMEOUT = 1 * 60 * 10000;

  public S3SinkStreamWriter(S3Folder streamS3Folder) throws IOException {
    this.streamS3Folder = streamS3Folder;
    segmentName = "segment-" + UUID.randomUUID().toString();

    writer = createNewWriter();
  }

  @Override
  public void append(Record record) throws Exception {
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(record);
    writer.write(bytes);
  }

  // finish up with the previous segment writer
  @Override
  public void prepareCommit() throws Exception {
    writer.waitAndClose(TIMEOUT);
  }

  //start of writing to a new segment
  @Override
  public void completeCommit() throws Exception {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.addUserMetadata("transaction", "complete");
    
    streamS3Folder.updateObjectMetadata(segmentName, metadata);

    writer = createNewWriter();
  }

  @Override
  public void commit() throws Exception {
    try {
      prepareCommit();
      completeCommit();
    } catch (Exception ex) {
      rollback();
      throw ex;
    }
  }

  //discard the uncommited buffer
  @Override
  public void rollback() throws Exception {
    streamS3Folder.deleteObject(segmentName);

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("application/binary");
    metadata.addUserMetadata("transaction", "prepare");

    writer = streamS3Folder.createObjectWriter(segmentName, metadata);
  }

  private S3ObjectWriter createNewWriter() throws IOException {
    segmentName = "segment-" + UUID.randomUUID().toString();
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.addUserMetadata("transaction", "prepare");
    writer = streamS3Folder.createObjectWriter(segmentName, metadata);
    return writer;
  }

  @Override
  public void close() throws Exception {
    writer.waitAndClose(TIMEOUT);
  }
}
