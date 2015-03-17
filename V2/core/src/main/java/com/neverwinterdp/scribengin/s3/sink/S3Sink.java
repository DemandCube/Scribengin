package com.neverwinterdp.scribengin.s3.sink;

import java.util.LinkedHashMap;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.neverwinterdp.scribengin.s3.S3Client;
import com.neverwinterdp.scribengin.s3.S3Folder;
import com.neverwinterdp.scribengin.s3.S3Util;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class S3Sink implements Sink {
  private SinkDescriptor descriptor ;
  private S3Folder       sinkFolder ;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, S3SinkStream> streams = new LinkedHashMap<Integer, S3SinkStream>() ;
  
  public S3Sink(S3Client s3Client, SinkDescriptor  descriptor) {
    this.descriptor = descriptor;
    String bucketName = descriptor.attribute("s3.bucket.name");
    if(!s3Client.hasBucket(bucketName)) {
      throw new  AmazonServiceException("Bucket " + bucketName + " does not exist");
    }
    String folderPath = descriptor.attribute("s3.storage.path");
    if(!s3Client.hasKey(bucketName, folderPath)) {
      s3Client.createS3Folder(bucketName, folderPath) ;
    }
    sinkFolder = s3Client.getS3Folder(bucketName, folderPath);
    
    List<String> streamNames = sinkFolder.getChildrenNames();
    for(String streamName : streamNames) {
      SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor(descriptor);
      streamDescriptor.attribute("s3.stream.name", streamName);
      streamDescriptor.setId(S3Util.getStreamId(streamName));
      S3SinkStream stream = new S3SinkStream(sinkFolder, streamDescriptor);
      streams.put(stream.getDescriptor().getId(), stream);
      if(idTracker < stream.getDescriptor().getId()) {
        idTracker = stream.getDescriptor().getId();
      }
    }
  }
  
  public S3Folder getSinkFolder() { return this.sinkFolder ; }
  
  @Override
  public SinkDescriptor getDescriptor() { return descriptor; }

  @Override
  synchronized public SinkStream getStream(SinkStreamDescriptor descriptor) throws Exception {
    return streams.get(descriptor.getId());
  }

  @Override
  synchronized public SinkStream[] getStreams() {
    SinkStream[] array = new SinkStream[streams.size()];
    return streams.values().toArray(array);
  }

  //TODO: Should consider a sort of transaction to make the operation reliable
  @Override
  synchronized public void delete(SinkStream stream) throws Exception {
    SinkStream found = streams.remove(stream.getDescriptor().getId());
    if(found != null) {
      found.delete();
    }
  }

  @Override
  synchronized public SinkStream newStream() throws Exception {
    int streamId = ++idTracker ;
    SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor(descriptor);
    streamDescriptor.setId(streamId);
    streamDescriptor.attribute("s3.stream.name", "stream-" + streamId);
    S3SinkStream stream = new S3SinkStream(sinkFolder, streamDescriptor);
    streams.put(streamId, stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception {
  }
}
