package com.neverwinterdp.scribengin.storage.s3.source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;

/**
 * @author Anthony Musyoki
 */
public class S3Source implements Source {

  private StorageDescriptor descriptor;
  private Map<Integer, S3SourceStream> streams = new LinkedHashMap<Integer, S3SourceStream>();

  public S3Source(S3Client s3Client, String location) throws Exception {
    this(s3Client, new StorageDescriptor("s3", location));
  }

  public S3Source(S3Client s3Client, StreamDescriptor streamDescriptor) throws Exception {
    this(s3Client, getSourceDescriptor(streamDescriptor));
  }

  public S3Source(S3Client s3Client, StorageDescriptor descriptor) throws Exception {
    String bucket = descriptor.getLocation();
    this.descriptor = descriptor;
    
    //TODO(tuan) do we create the bucket or die?
    if (!s3Client.hasBucket(bucket)) {
      //  s3Client.createBucket(descriptor.getLocation());
      throw new Exception("bucket " + bucket + " does not exist!");
    }
    
    // a source stream for every folder in the bucket
    List<S3Folder> folders = s3Client.getRootFolders(bucket);
    int id = 0;
    for (S3Folder s3Folder : folders) {
      StreamDescriptor sDescriptor = new StreamDescriptor();
      sDescriptor.setType(descriptor.getType());
      sDescriptor.setLocation(s3Folder.getFolderPath());
      sDescriptor.setId(id++);
      sDescriptor.attribute("s3.bucket.name", descriptor.attribute("s3.bucket.name"));
      sDescriptor.attribute("s3.storage.path", s3Folder.getFolderPath());
      S3SourceStream stream = new S3SourceStream(s3Client, sDescriptor);
      streams.put(sDescriptor.getId(), stream);
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
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.setType(streamDescriptor.getType());
    String location = streamDescriptor.getLocation();
    location = location.substring(0, location.lastIndexOf('/'));
    descriptor.setLocation(location);
    return descriptor;
  }
}
