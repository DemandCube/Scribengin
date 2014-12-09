package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.util.LinkedList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkStream.
 */
public class S3SinkStream implements SinkStream {

  /** The name. */
  private String name;
  
  /** The s3. */
  private AmazonS3 s3;
  
  /** The bucket name. */
  private String bucketName;
  
  /** The partititioner. */
  private SinkPartitioner partititioner;
  
  /** The buffer. */
  private LinkedList<Tuple> buffer;
  
  /** The max memory buffer size. */
  private int maxMemoryBufferSize;
  
  /** The max memory buffering time. */
  private int maxMemoryBufferingTime;
  
  /** The max tupples in memory. */
  private long maxTupplesInMemory;
  
  /** The max disk buffer size. */
  private int maxDiskBufferSize;
  
  /** The max disk buffering time. */
  private int maxDiskBufferingTime;
  
  /** The max tupples in disk. */
  private long maxTupplesInDisk;
  
  /** The max tuples in memory listner. */
  private SinkListner maxTuplesInMemoryListner;
  
  /** The max memory buffer size listner. */
  private SinkListner maxMemoryBufferSizeListner;
  
  /** The max memory buffering time listner. */
  private SinkListner maxMemoryBufferingTimeListner;
  
  /** The max tuples in disk listner. */
  private SinkListner maxTuplesInDiskListner;
  
  /** The max disk buffer size listner. */
  private SinkListner maxDiskBufferSizeListner;
  
  /** The max disk buffering time listner. */
  private SinkListner maxDiskBufferingTimeListner;
  
  /** The tuples size. */
  private long tuplesSize;
  
  /** The star time. */
  private long starTime = 0;
  
  /** The disk buffer. */
  private DiskBuffer diskBuffer;
  
  /** The memory buffer. */
  private boolean memoryBuffer = true;
  
  /** The local tmp dir. */
  private String localTmpDir;

  /**
   * The Constructor.
   *
   * @param partititioner the partititioner
   * @param bucketName the bucket name
   * @param localTmpDir the local tmp dir
   * @param regionName the region name
   * @param chunkSize the chunk size
   */
  public S3SinkStream(SinkPartitioner partititioner, String bucketName, String localTmpDir, Regions regionName, int chunkSize) {
    
    this.partititioner = partititioner;
    this.bucketName = bucketName;
    this.localTmpDir = localTmpDir;
    // this.diskBuffer = new SimpleFileBuffer();
    this.diskBuffer = new FileChannelBuffer(this.partititioner, 1024 * 10, chunkSize);
    buffer = new LinkedList<Tuple>();
    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }

    s3 = new AmazonS3Client(credentials);
    Region region = Region.getRegion(regionName);
    s3.setRegion(region);
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.Stream#prepareCommit()
   */
  @Override
  public boolean prepareCommit() {
    // check if bucket exist
    if (!s3.doesBucketExist(bucketName)) {
      s3.createBucket(bucketName);
    }
    return true;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.Stream#commit()
   */
  @Override
  public boolean commit() {
    if (buffer.size() > 0)
      bufferToDisk();

    String path;
    for (File file : diskBuffer.getAllFiles()) {
      // the file path on local is similar to its path on s3, just change tmp folder by bucket name
      path = file.getPath();
      String key = path.substring(path.lastIndexOf("/") + 1, path.length());
      String folder = path.substring(0, path.lastIndexOf("/"));
      folder = folder.replaceFirst(localTmpDir, bucketName);
      System.out.println("Uploading a new object to S3 from a file\n");
      PutObjectRequest object = new PutObjectRequest(folder, key, file);
      try {
        // upload to S3
        s3.putObject(object);
      } catch (AmazonServiceException ase) {
        System.out.println("Caught an AmazonServiceException, which means your request made it ");
      } catch (AmazonClientException ace) {
        System.out.println("Caught an AmazonClientException, which means the client encountered .");

      }
    }

    return true;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.Stream#clearBuffer()
   */
  @Override
  public boolean clearBuffer() {
    buffer.clear();
    return true;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.Stream#completeCommit()
   */
  @Override
  public boolean completeCommit() {
    buffer.clear();
    diskBuffer.clean();
    return true;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#bufferTuple(com.neverwinterdp.scribengin.tuple.Tuple)
   */
  @Override
  public boolean bufferTuple(Tuple tuple) {
    // set the first time the buffering in memory start
    if (starTime == 0) {
      starTime = System.currentTimeMillis();
    }
    // if buffering is in memory
    if (memoryBuffer) {
      // add tuple to buffer
      buffer.add(tuple);
      // set the total size of data in bytes
      tuplesSize += tuple.getData().length;
      // fire event if the number of tuples in memory exceed the maximum number allowed
      if (maxTuplesInMemoryListner != null && buffer.size() >= maxTupplesInMemory) {
        maxTuplesInMemoryListner.run();
      }
      //fire event if data size of all tuples in memory exceed the max size allowed
      if (maxMemoryBufferSizeListner != null && tuplesSize > maxMemoryBufferSize) {
        maxMemoryBufferSizeListner.run();
      }
      //fire event if buffering in memory take time more than max period allowed
      if (maxMemoryBufferingTimeListner != null && System.currentTimeMillis() - starTime > maxMemoryBufferingTime) {
        maxMemoryBufferingTimeListner.run();
      }
    // if buffering is only on disk
    } else {
      
      diskBuffer.add(tuple);
      // fire event if the number of tuples on disk exceed the maximum number allowed
      if (maxTuplesInDiskListner != null && diskBuffer.getTuplesCount() >= maxTupplesInDisk) {
        maxTuplesInDiskListner.run();
      }
    //fire event if data size of all tuples on disk exceed the max size allowed
      if (maxDiskBufferSizeListner != null && diskBuffer.getTuplesSize() > maxDiskBufferSize) {
        maxDiskBufferSizeListner.run();
      }
      //fire event if buffering on disk take time more than max period allowed
      if (maxDiskBufferingTimeListner != null && diskBuffer.getDuration() > maxDiskBufferingTime) {
        maxDiskBufferingTimeListner.run();
      }
      
    }

    return true;

  }

  /**
   * Buffer to disk.
   */
  public void bufferToDisk() {
    // loop tuples in memory and write them to disk
    for (Tuple tuple : buffer) {
      diskBuffer.add(tuple); 
    }
    buffer.clear();
    if (maxTuplesInDiskListner != null && diskBuffer.getTuplesCount() >= maxTupplesInDisk) {
      maxTuplesInDiskListner.run();
    }
    if (maxDiskBufferSizeListner != null && diskBuffer.getTuplesSize() > maxDiskBufferSize) {
      maxDiskBufferSizeListner.run();
    }
    if (maxDiskBufferingTimeListner != null && diskBuffer.getDuration() > maxDiskBufferingTime) {
      maxDiskBufferingTimeListner.run();
    }

  }

  /**
   * Sets the max memory buffer size.
   *
   * @param maxMemoryBufferSize the max memory buffer size
   */
  public void setMaxMemoryBufferSize(int maxMemoryBufferSize) {
    this.maxMemoryBufferSize = maxMemoryBufferSize;
  }

  /**
   * Sets the max memory buffering time.
   *
   * @param maxMemoryBufferingTime the max memory buffering time
   */
  public void setMaxMemoryBufferingTime(int maxMemoryBufferingTime) {
    this.maxMemoryBufferingTime = maxMemoryBufferingTime * 1000;
  }

  /**
   * Sets the max tupples in memory.
   *
   * @param maxTupplesInMemory the max tupples in memory
   */
  public void setMaxTupplesInMemory(long maxTupplesInMemory) {
    this.maxTupplesInMemory = maxTupplesInMemory;
  }

  /**
   * Sets the max disk buffer size.
   *
   * @param maxDiskBufferSize the max disk buffer size
   */
  public void setMaxDiskBufferSize(int maxDiskBufferSize) {
    this.maxDiskBufferSize = maxDiskBufferSize;
  }

  /**
   * Sets the max disk buffering time.
   *
   * @param maxDiskBufferingTime the max disk buffering time
   */
  public void setMaxDiskBufferingTime(int maxDiskBufferingTime) {
    this.maxDiskBufferingTime = maxDiskBufferingTime *1000;
  }

  /**
   * Sets the max tupples in disk.
   *
   * @param maxTupplesInDisk the max tupples in disk
   */
  public void setMaxTupplesInDisk(long maxTupplesInDisk) {
    this.maxTupplesInDisk = maxTupplesInDisk;
  }

  
  /**
   * Adds the on max tuples in momory listner.
   *
   * @param maxTuplesListner the max tuples listner
   */
  public void addOnMaxTuplesInMomoryListner(SinkListner maxTuplesListner) {
    this.maxTuplesInMemoryListner = maxTuplesListner;
  }

  /**
   * Adds the on max memory buffer size listner.
   *
   * @param maxBufferSizeListner the max buffer size listner
   */
  public void addOnMaxMemoryBufferSizeListner(SinkListner maxBufferSizeListner) {
    this.maxMemoryBufferSizeListner = maxBufferSizeListner;
  }

  /**
   * Adds the on max memory buffering time listner.
   *
   * @param maxBufferingTimeListner the max buffering time listner
   */
  public void addOnMaxMemoryBufferingTimeListner(SinkListner maxBufferingTimeListner) {
    this.maxMemoryBufferingTimeListner = maxBufferingTimeListner;
  }

  /**
   * Adds the on max tuples in disk listner.
   *
   * @param maxTuplesListner the max tuples listner
   */
  public void addOnMaxTuplesInDiskListner(SinkListner maxTuplesListner) {
    this.maxTuplesInDiskListner = maxTuplesListner;
  }

  /**
   * Adds the on disk buffer size listner.
   *
   * @param maxBufferSizeListner the max buffer size listner
   */
  public void addOnDiskBufferSizeListner(SinkListner maxBufferSizeListner) {
    this.maxDiskBufferSizeListner = maxBufferSizeListner;
  }

  /**
   * Adds the on max disk buffering time listner.
   *
   * @param maxBufferingTimeListner the max buffering time listner
   */
  public void addOnMaxDiskBufferingTimeListner(SinkListner maxBufferingTimeListner) {
    this.maxDiskBufferingTimeListner = maxBufferingTimeListner;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#rollBack()
   */
  @Override
  public boolean rollBack() {
    return true;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#setSinkPartitioner(com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner)
   */
  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    this.partititioner = sp;

  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#getBufferSize()
   */
  @Override
  public long getBufferSize() {
    return this.buffer.size() + diskBuffer.getTuplesCount();
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#getName()
   */
  @Override
  public String getName() {
    return name;
  }

}
