package com.neverwinterdp.scribengin.sink;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.AbstractModule;
import com.neverwinterdp.scribengin.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;

/**
 * The Class S3Module.
 */
public class S3Module extends AbstractModule {

  /** The prop file path. */
  private String propFilePath;

  /** The s3 sink config. */
  private S3SinkConfig s3SinkConfig;

  /** The kafka partition. */
  private int kafkaPartition;

  /** The topic. */
  private String topic;

  /** The mock. */
  private boolean mock = true;

  /**
   * The Constructor.
   * 
   * @param propFilePath
   *        the prop file path
   * @param topic
   *        the topic
   * @param kafkaPartition
   *        the kafka partition
   */
  public S3Module(String propFilePath, String topic, int kafkaPartition, boolean mock) {
    this.propFilePath = propFilePath;
    this.topic = topic;
    this.kafkaPartition = kafkaPartition;
    this.mock = mock;
  }

  /*
   * (non-Javadoc)
   * @see com.google.inject.AbstractModule#configure()
   */
  @Override
  protected void configure() {
    if (mock) {
      AWSCredentials credentials = new BasicAWSCredentials("", "");
      bind(AmazonS3.class).toInstance(new AmazonS3Mock(credentials));

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
    s3SinkConfig = new S3SinkConfig(propFilePath);
    System.out.println("region "+s3SinkConfig.getRegionName());
    SinkPartitioner sp =
        new OffsetPartitioner(s3SinkConfig.getOffsetPerPartition(), topic, kafkaPartition);
    bind(SinkPartitioner.class).toInstance(sp);
    bind(S3SinkConfig.class).toInstance(s3SinkConfig);
  }
}
