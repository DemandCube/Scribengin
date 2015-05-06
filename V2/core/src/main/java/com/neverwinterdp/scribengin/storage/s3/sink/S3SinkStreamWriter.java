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

  public S3SinkStreamWriter(S3Folder streamS3Folder) throws IOException {
    this.streamS3Folder = streamS3Folder;
    segmentName = "segment-" + UUID.randomUUID().toString();
    
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("application/binary");
    metadata.addUserMetadata("transaction", "prepare");
    //TODO(Tuan) why we don't check to see if segmentName exists?
    writer = streamS3Folder.createObjectWriter(segmentName, metadata);
  }

  @Override
  public void append(Record record) throws Exception {
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(record);
    writer.write(bytes);
  }

  //TODO(Tuan) shouldn't we create another writer segment here?
  @Override
  public void prepareCommit() throws Exception {
       writer.waitAndClose(1 * 60 * 1000);
  }

  @Override
  public void completeCommit() throws Exception {
    ObjectMetadata metadata = writer.getObjectMetadata();
    metadata.addUserMetadata("transaction", "complete");
    streamS3Folder.updateObjectMetadata(segmentName, metadata);
  }

  @Override
  public void commit() throws Exception {
    try {
    prepareCommit();
    completeCommit();
    } catch(Exception ex) {
      rollback();
      throw ex;
    }
  }

  @Override
  public void rollback() throws Exception {

  }
  
//TODO writer.close() here?
  @Override
  public void close() throws Exception {
    
  }
}
