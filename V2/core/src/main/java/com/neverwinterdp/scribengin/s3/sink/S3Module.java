package com.neverwinterdp.scribengin.s3.sink;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.AbstractModule;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;

/**
 * The Class S3Module.
 */
public class S3Module extends AbstractModule {

  /** The s3 sink config. */
  private S3SinkConfig s3SinkConfig;

  /**
   * The Constructor.
   * 
   * @param propFilePath
   *          the prop file path
   * @param topic
   *          the topic
   * @param kafkaPartition
   *          the kafka partition
   */
  public S3Module(SinkStreamDescriptor propMap) {
    s3SinkConfig = new S3SinkConfig(propMap);

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.google.inject.AbstractModule#configure()
   */
  @Override
  protected void configure() {

    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }
    bind(AmazonS3.class).toInstance(new AmazonS3Client(credentials));
    SinkPartitioner sp = new OffsetPartitioner(s3SinkConfig.getOffsetPerPartition());
    bind(SinkPartitioner.class).toInstance(sp);
    bind(S3SinkConfig.class).toInstance(s3SinkConfig);
  }
}
