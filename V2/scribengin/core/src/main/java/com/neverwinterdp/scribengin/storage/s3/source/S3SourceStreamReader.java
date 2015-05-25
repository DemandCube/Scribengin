package com.neverwinterdp.scribengin.storage.s3.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class S3SourceStreamReader implements SourceStreamReader {

  private String name;
  private S3Client s3Client;
  private S3Object s3Object;
  private S3ObjectReader recordObjectReader;

  private int commitPoint;
  private int currPosition;
  private CommitPoint lastCommitInfo;

  public S3SourceStreamReader(String name, S3Client client, StreamDescriptor descriptor) throws FileNotFoundException,
      IllegalArgumentException, IOException {
    this.name = name;
    this.s3Client = client;

    GetObjectRequest objectRequest = new GetObjectRequest(descriptor.attribute("s3.bucket.name"),
        descriptor.attribute("s3.stream.name"));
    s3Object = s3Client.getAmazonS3Client().getObject(objectRequest);

    recordObjectReader = getObjectReader();
  }

  public String getName() {
    return name;
  }

  public Record next() throws Exception {
    if (recordObjectReader == null) {
      return null;
    }
    if(recordObjectReader.hasNext()) {
      return recordObjectReader.next();
    }
    else return null;
  }

  public Record[] next(int size) throws Exception {
    List<Record> holder = new ArrayList<Record>();
    Record[] array = new Record[holder.size()];
    for (int i = 0; i < size; i++) {
      Record record = next();
      if (record != null)
        holder.add(record);
      else
        break;
    }
    holder.toArray(array);
    return array;
  }

  public void rollback() throws Exception {
    System.err.println("rollback() This method is not implemented");
    currPosition = commitPoint;
  }

  @Override
  public void prepareCommit() {
    
  }

  @Override
  public void completeCommit() {
   
  }

  public void commit() throws Exception {
    System.err.println("commit() This method is not implemented");
    lastCommitInfo = new CommitPoint(name, commitPoint, currPosition);
    this.commitPoint = currPosition;
  }

  public CommitPoint getLastCommitInfo() {
    return this.lastCommitInfo;
  }

  public void close() throws Exception {
  }

  private S3ObjectReader getObjectReader() throws IOException {
    S3ObjectReader reader = new S3ObjectReader(s3Object.getObjectContent());

    return reader;
  }
}