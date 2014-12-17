package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Permission;
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
  private AmazonS3 s3Client;

  /** The bucket name. */
  private String bucketName;

  /** The partitioner. */
  private SinkPartitioner partitioner;

  /** The memory buffer. */
  private Buffer buffer;

  /** The local tmp dir. */
  private String localTmpDir;

  /** The logger. */
  private static Logger logger;

  /** The region name. */
  private Regions regionName;

  /** The valid s3 sink. */
  private boolean validS3Sink = false;

  /** The time to commit. */
  private boolean full = false;

  /** The uploaded files path. */
  private List<String> uploadedFilesPath = new ArrayList<>();

  private AWSCredentials credentials;
  /**
   * The Constructor.
   *
   * @param partitioner
   *          the partitioner
   * @param bucketName
   *          the bucket name
   * @param localTmpDir
   *          the local temporary directory
   * @param regionName
   *          the region name
   * @param chunkSize
   *          the number of Tuples per file
   */
  public S3SinkStream(SinkPartitioner partitioner, S3SinkConfig config) {
    logger = LoggerFactory.getLogger("S3SinkStream");
    this.partitioner = partitioner;
    this.bucketName = config.getBucketName();
    this.localTmpDir = config.getLocalTmpDir();
    this.buffer = new Buffer(partitioner, config);
    credentials = new ProfileCredentialsProvider().getCredentials();
    s3Client = new AmazonS3Client(credentials);
    this.regionName = Regions.fromName(config.getRegionName());
    Region region = Region.getRegion(regionName);
    s3Client.setRegion(region);
  }

  /**
   * Checks if it is time to commit.
   *
   * @return true, if checks if is time to commit
   */
  public boolean isFull() {
    return full;
  }

  /**
   * Sets the s3 client.
   *
   * @param s3Client the s3 client
   */
  public void setS3Client(AmazonS3 s3Client) {
    this.s3Client = s3Client;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#prepareCommit()
   */
  @Override
  public boolean prepareCommit() {

    if (!validS3Sink) {

      try {
        // check if bucket exist
        if (!s3Client.doesBucketExist(bucketName)) {
          s3Client.createBucket(bucketName);
          validS3Sink = true;
        } else {
          AccessControlList acl = s3Client.getBucketAcl(bucketName);
          List<Permission> permissions = new ArrayList<Permission>();
          for (Grant grant : acl.getGrants()) {
            permissions.add(grant.getPermission());
          }
          if (permissions.contains(Permission.FullControl) || permissions.contains(Permission.Write)) {
            validS3Sink = true;
          }
        }
      } catch (Exception e) {
        logger.error("Caught Exception when perparing commit " + e.getMessage());
        validS3Sink = false;
      }
    } else {
      validS3Sink = true;
      ;
    }
    return validS3Sink;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#commit()
   */
  @Override
  public boolean commit() {
    String path;
    File file;
    while (buffer.getFilesSize() > 0) {
      // the file path on local is similar to its path on s3, just change tmp
      // folder by bucket name
      file = buffer.pollFromDisk();
      path = file.getPath();
      String key = path.substring(path.lastIndexOf("/") + 1, path.length());
      String folder = path.substring(0, path.lastIndexOf("/"));
      folder = folder.replaceFirst(localTmpDir, bucketName);
      logger.info("Uploading a new object to S3 from a file\n");
      PutObjectRequest object = new PutObjectRequest(folder, key, file);
      try {
        // upload to S3
        s3Client.putObject(object);
        uploadedFilesPath.add(folder + "/" + key);
      } catch (AmazonServiceException ase) {
        logger.error("Caught an AmazonServiceException. " + ase.getMessage());
        return false;
      } catch (AmazonClientException ace) {
        logger.error("Caught an AmazonClientException. " + ace.getMessage());
        return false;
      }
    }

    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#clearBuffer()
   */
  @Override
  public boolean clearBuffer() {
    buffer.clean();
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#completeCommit()
   */
  @Override
  public boolean completeCommit() {
    buffer.clean();
    uploadedFilesPath.clear();
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#bufferTuple(com.
   * neverwinterdp.scribengin.tuple.Tuple)
   */
  @Override
  public boolean bufferTuple(Tuple tuple) {
    buffer.add(tuple);
    return true;
  }



  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#rollBack()
   */
  @Override
  public boolean rollBack() {
    boolean retVal = true;
    for (String path : uploadedFilesPath) {
      try{
        s3Client.deleteObject(bucketName, path);
      }
      catch(Exception e){
        logger.error("Caught Exception when rolling back " + e.getMessage());
        retVal = false;
      }
    }
    uploadedFilesPath.clear();
    return retVal;

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.neverwinterdp.scribengin.stream.sink.SinkStream#setSinkPartitioner(
   * com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner)
   */
  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    this.partitioner = sp;

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#getBufferSize()
   */
  @Override
  public long getBufferSize() {
    return buffer.getTuplesCount();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#getName()
   */
  @Override
  public String getName() {
    return name;
  }

}
