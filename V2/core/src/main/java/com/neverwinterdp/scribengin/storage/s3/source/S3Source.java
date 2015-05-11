package com.neverwinterdp.scribengin.storage.s3.source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;

/**
 * @author Anthony Musyoki
 * 
 * For Consistency with S3Sink a Source is a folder in a bucket, a SourceStream is a file in the folder, a SourceStreamReader read the file
 */
public class S3Source implements Source {

  private S3Folder sourceFolder;
  private StorageDescriptor descriptor;
  private int streamId = 0;
  private Map<Integer, S3SourceStream> streams = new LinkedHashMap<Integer, S3SourceStream>();

  public S3Source(S3Client s3Client, StreamDescriptor streamDescriptor) throws Exception {

    this(s3Client, getSourceDescriptor(streamDescriptor));

  }

  public S3Source(S3Client s3Client, StorageDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    String bucketName = descriptor.attribute("s3.bucket.name");

    if (!s3Client.hasBucket(bucketName)) {
      throw new AmazonServiceException("Bucket " + bucketName + " does not exist.");
    }

    String folderPath = descriptor.attribute("s3.storage.path");
    if (!s3Client.hasKey(bucketName, folderPath)) {
      throw new AmazonServiceException("Folder " + folderPath + " does not exist.");
    }

    sourceFolder = s3Client.getS3Folder(bucketName, folderPath);
    List<S3ObjectSummary> streamNames = sourceFolder.getDescendants();
    for (S3ObjectSummary streamName : streamNames) {
      if (Long.valueOf(streamName.getSize()).compareTo(0l) > -0) {
        StreamDescriptor streamDescriptor = new StreamDescriptor(descriptor);
        streamDescriptor.attribute("s3.stream.name", streamName.getKey());
        streamDescriptor.setId(streamId++);
        S3SourceStream stream = new S3SourceStream(s3Client, streamDescriptor);
        streams.put(stream.getDescriptor().getId(), stream);
      }
    }
  }

  public StorageDescriptor getDescriptor() {
    return descriptor;
  }

  public SourceStream getStream(int id) {
    return streams.get(id);
  }

  public SourceStream getStream(StreamDescriptor descriptor) {
    return streams.get(descriptor.getId());
  }

  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[streams.size()];
    return streams.values().toArray(array);
  }

  public void close() throws Exception {
  }

  static StorageDescriptor getSourceDescriptor(StreamDescriptor streamDescriptor) {
    return streamDescriptor;
  }
}
