package com.neverwinterdp.scribengin.nizarS3.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.sink.partitioner.SinkPartitioner;

public class S3SinkStreamWriter implements SinkStreamWriter {

  /** The s3. */
  private AmazonS3 s3Client;

  /** The bucket name. */
  private String bucketName;
  
  private String prefix;

  /** The partitioner. */
  private SinkPartitioner partitioner;

  /** The memory buffer. */
  private S3SinkBuffer buffer;

  /** The local tmp dir. */
  private String localTmpDir;

  /** The logger. */
  private static Logger logger;

  /** The region name. */
  private Regions regionName;

  /** The valid s3 sink. */
  private boolean validS3Sink = false;

  /** A map holding file-path name and it corresponding MD5 hash */
  private Map<String, String> uploadedFilesPath = new HashMap<>();


  /**
   * The Constructor.
   * 
   * @param s3Client
   *          the s3 client
   * @param partitioner
   *          the partitioner
   * @param config
   *          the configuration
   */
  @Inject
  public S3SinkStreamWriter(AmazonS3 s3Client, SinkPartitioner partitioner, S3SinkConfig config) {
    logger = LogManager.getLogger(S3SinkStream.class);
    this.partitioner = partitioner;
    this.bucketName = config.getBucketName();
    this.prefix = config.getPrefix();
    this.localTmpDir = config.getLocalTmpDir();
    this.buffer = new S3SinkBuffer(this.partitioner, config);
    this.s3Client = s3Client;
    this.regionName = Regions.fromName(config.getRegionName());
    Region region = Region.getRegion(regionName);
    this.s3Client.setRegion(region);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#prepareCommit()
   */
  // This is where we create the bucket.
  @Override
  public void prepareCommit() throws Exception {
    logger.info("prepareCommit");
    if (!validS3Sink) {

      // check if bucket exist
      if (!s3Client.doesBucketExist(bucketName)) {
        System.out.println("bucket does not exist.");
        logger.info("Bucket does not Exist");
        s3Client.createBucket(bucketName);

      }

      logger.info("Bucket Exist");
      /*
       * BucketVersioningConfiguration configuration = new
       * BucketVersioningConfiguration( bucketVersionConfig);
       * SetBucketVersioningConfigurationRequest request = new
       * SetBucketVersioningConfigurationRequest( bucketName, configuration);
       * s3Client.setBucketVersioningConfiguration(request);
       */
      AccessControlList acl = s3Client.getBucketAcl(bucketName);
      List<Permission> permissions = new ArrayList<Permission>();
      for (Grant grant : acl.getGrants()) {
        permissions.add(grant.getPermission());
      }
      if (permissions.contains(Permission.FullControl) || permissions.contains(Permission.Write)) {
        validS3Sink = true;
      }

    } else {
      validS3Sink = true;
    }
    logger.info("validS3Sink = " + validS3Sink);
    System.out.println("validS3Sink = " + validS3Sink);

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#commit()
   */
  @Override
  public void commit() throws Exception {
    System.out.println("committing");
    logger.info("commit");
    buffer.purgeMemoryToDisk();
    String path;
    File file;
    while (buffer.getFilesCount() > 0) {
      // the file path on local is similar to its path on s3, just change
      // tmp folder by bucket name
      file = buffer.pollFromDisk();
      if(!file.exists())
        throw new FileNotFoundException();
      System.out.println("getFilesCount " + buffer.getFilesCount() + " ");

      path = file.getPath();
      logger.info("file path " + path);
      String key = (prefix.equals("") ? "" :prefix + "/" ) + path.substring(path.indexOf(localTmpDir) + localTmpDir.length() +1 , path.length());
      System.out.println("path exists " + path);
      System.out.println("key " + key);

      logger.info("Uploading a new object to S3 from a file\n");
      try {
        // upload to S3
        PutObjectResult result = s3Client.putObject(bucketName, key, file);

        System.out.println("result " + result.getContentMd5());
        uploadedFilesPath.put(key, result.getContentMd5());
      } catch (AmazonServiceException ase) {
        logger.error("Caught an AmazonServiceException. " + ase.getMessage());
        if (ase.getCause() instanceof IOException) {
          throw (IOException) ase.getCause();
        }
        System.out.println(ase);
        Throwables.propagate(ase);

      } catch (AmazonClientException ace) {
        logger.error("Caught an AmazonClientException. " + ace.getMessage());
        if (ace.getCause() instanceof IOException) {
          throw (IOException) ace.getCause();
        }
        System.out.println(ace);
        Throwables.propagate(ace);
      }
    }
   
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.Stream#completeCommit()
   */
  @Override
  // TODO check that files have correctly been uploaded to s3?
  public void completeCommit() throws Exception {
    logger.info("completeCommit");
    buffer.clear();
    uploadedFilesPath.clear();

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#bufferRecord(com.
   * neverwinterdp.scribengin.tuple.Record)
   */
  @Override
  public void append(Record tuple) throws Exception{
    buffer.add(tuple);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#rollBack()
   */
  // TODO if we are using versioning delete the version
  @Override
  public void rollback() throws Exception {

    // TODO remove from uploadedFilesPath
    for (String path : uploadedFilesPath.keySet()) {
      System.out.println("deleted " + path);
      s3Client.deleteObject(bucketName, path);
    }
    uploadedFilesPath.clear();

  }
  
  @Override
  public void close() throws Exception {
    logger.info("clear buffer");
    try {
      buffer.clear();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
  public void setSinkPartitioner(SinkPartitioner sinkPartitioner) {
    this.partitioner = sinkPartitioner;

  }

  public S3SinkBuffer getBuffer() {
    return buffer;
  }

  public AmazonS3 getAmazonS3() {
    return s3Client;
  }

  public Map<String, String> getUploadedFilePaths() {
    return uploadedFilesPath;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

}