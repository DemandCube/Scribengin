package com.neverwinterdp.scribengin.nizarS3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.AbstractModule;
import com.neverwinterdp.scribengin.nizarS3.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;

/**
 * The Class S3Module.
 */
public class S3TestModule extends AbstractModule {

  /** The s3 sink config. */
  private S3SinkConfig         s3SinkConfig;

  /** The mock. */
  private boolean              mock = true;

  private SinkStreamDescriptor descriptor;

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
  public S3TestModule(SinkStreamDescriptor descriptor) {
    s3SinkConfig = new S3SinkConfig(descriptor);
    this.mock = false;
  }

  public S3TestModule(SinkStreamDescriptor descriptor, boolean mock) {
    this.descriptor = descriptor;
    s3SinkConfig = new S3SinkConfig(descriptor);

    this.mock = mock;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.google.inject.AbstractModule#configure()
   */
  @Override
  protected void configure() {
    if (mock) {
      AWSCredentials credentials = new BasicAWSCredentials("", "");
      AmazonS3Mock amazonS3Mock = new AmazonS3Mock(credentials);
      amazonS3Mock.createBucket(descriptor.get("bucketName"));
      bind(AmazonS3.class).toInstance(amazonS3Mock);

    } else {
      AWSCredentials credentials = null;
      try {
        credentials = new ProfileCredentialsProvider().getCredentials();
      } catch (Exception e) {
        throw new AmazonClientException(
            "Cannot load the credentials from the credential profiles file. ", e);
      }
      bind(AmazonS3.class).toInstance(new AmazonS3Client(credentials));
    }

    System.out.println("region " + s3SinkConfig.getRegionName());
    SinkPartitioner sp =
        new OffsetPartitioner(s3SinkConfig.getOffsetPerPartition());
    bind(SinkPartitioner.class).toInstance(sp);
    bind(S3SinkConfig.class).toInstance(s3SinkConfig);
  }
}
