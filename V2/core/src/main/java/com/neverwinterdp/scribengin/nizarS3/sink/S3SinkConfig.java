package com.neverwinterdp.scribengin.nizarS3.sink;

import java.util.Properties;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;

public class S3SinkConfig {

  private final Properties props;

  public S3SinkConfig(Properties props) {
    this.props = props;
  }

  public S3SinkConfig(StreamDescriptor sinkStreamDescriptor) {
    props = new Properties();
    props.putAll(sinkStreamDescriptor);
    props.put("prefix" , sinkStreamDescriptor.getLocation());
  }

  
  public String getPrefix() {
    return getString("prefix");
  }
  /**
   * Gets the bucket name.
   * 
   * @return the bucket name
   */
  public String getBucketName() {
    return getString("bucketName");
  }

  /**
   * Gets the mapped byte buffer size.
   * 
   * @return the mapped byte buffer size
   */
  public int getMappedByteBufferSize() {
    return getInt("diskBuffer.mappedByteBufferSize");
  }

  /**
   * Gets the region name.
   * 
   * @return the region name
   */
  public String getRegionName() {
    return getString("regionName");
  }

  /**
   * Gets the local tmp dir.
   * 
   * @return the local tmp dir
   */
  public String getLocalTmpDir() {
    return System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + getString("localTmpDir");
  }

  /**
   * Gets the chunk size.
   * 
   * @return the chunk size
   */
  public int getChunkSize() {
    return getInt("chunkSize");
  }

  /**
   * Gets the memory max buffer size.
   * 
   * @return the memory max buffer size
   */
  public int getMemoryMaxBufferSize() {
    return getInt("memoryBuffer.maxBufferSize") * 1024;
  }

  /**
   * Gets the memory max buffering time.
   * 
   * @return the memory max buffering time
   */
  public int getMemoryMaxBufferingTime() {
    return getInt("memoryBuffer.maxBufferingTime") * 1000;
  }

  /**
   * Gets the memory max tuples.
   * 
   * @return the memory max tuples
   */
  public int getMemoryMaxRecords() {
    return getInt("memoryBuffer.maxRecords");
  }

  /**
   * Gets the disk max buffer size.
   * 
   * @return the disk max buffer size
   */
  public int getDiskMaxBufferSize() {
    return getInt("diskBuffer.maxBufferSize") * 1024;
  }

  /**
   * Gets the disk max buffering time.
   * 
   * @return the disk max buffering time
   */
  public int getDiskMaxBufferingTime() {
    return getInt("diskBuffer.maxBufferingTime") * 1000;
  }

  /**
   * Gets the disk max tuples.
   * 
   * @return the disk max tuples
   */
  public int getDiskMaxRecords() {
    return getInt("diskBuffer.maxRecords");
  }

  /**
   * Gets the offset per partition.
   * 
   * @return the offset per partition
   */
  public int getOffsetPerPartition() {
    return getInt("partitionner.chunkPerPartition") * getChunkSize();
  }

  /**
   * Checks if is memory buffering enabled.
   * 
   * @return true, if checks if is memory buffering enabled
   */
  public boolean isMemoryBufferingEnabled() {
    return getBoolean("memoryBuffer.enabled");
  }

  /**
   * Check property.
   * 
   * @param name
   *          the name
   */
  private void checkProperty(String name) {
    if (!props.containsKey(name)) {
      throw new IllegalArgumentException("Failed to find required configuration option '" + name + "'.");
    }
  }

  /**
   * Gets the string.
   * 
   * @param name
   *          the name
   * @return the string
   */
  private String getString(String name) {
    checkProperty(name);
    return props.getProperty(name);
  }

  /**
   * Gets the int.
   * 
   * @param name
   *          the name
   * @return the int
   */
  private int getInt(String name) {
    checkProperty(name);
    String property = props.getProperty(name);
    return Integer.parseInt(property);
  }

  /**
   * Gets the boolean.
   * 
   * @param name
   *          the name
   * @return the boolean
   */
  private boolean getBoolean(String name) {
    checkProperty(name);
    String property = props.getProperty(name);
    return Boolean.getBoolean(property);
  }

  public String getBucketVersioningConfig() {
    return getString("bucket.versioning.configuration");
  }
}
