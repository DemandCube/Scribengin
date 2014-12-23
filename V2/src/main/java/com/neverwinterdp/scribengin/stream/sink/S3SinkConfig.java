package com.neverwinterdp.scribengin.stream.sink;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

// TODO: Auto-generated Javadoc
/**
 * The Class S3SinkConfig.
 */
public class S3SinkConfig {

  /** The properties. */
  private final PropertiesConfiguration properties;

  /**
   * Exposed for testability.
   *
   * @param properties the properties
   */
  public S3SinkConfig(PropertiesConfiguration properties) {
    this.properties = properties;
  }
  
  /**
   * The Constructor.
   *
   * @param configProperty the config property
   */
  public S3SinkConfig(String configProperty) {
    Properties systemProperties = System.getProperties();
    systemProperties.getProperty("config");
    try {
      properties = new PropertiesConfiguration(configProperty);

    } catch (ConfigurationException e) {
      throw new RuntimeException("Error loading configuration from " + configProperty);
    }

    for (final Map.Entry<Object, Object> entry : systemProperties.entrySet()) {
      properties.setProperty(entry.getKey().toString(), entry.getValue());
    }
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
  public int getMappedByteBufferSize(){
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
    return getString("localTmpDir");
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
    return getInt("memoryBuffer.maxBufferingTime") *1000;
  }
  
  /**
   * Gets the memory max tuples.
   *
   * @return the memory max tuples
   */
  public int getMemoryMaxTuples() {
    return getInt("memoryBuffer.maxTuples");
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
    return getInt("diskBuffer.maxBufferingTime") *1000;
  }
  
  /**
   * Gets the disk max tuples.
   *
   * @return the disk max tuples
   */
  public int getDiskMaxTuples() {
    return getInt("diskBuffer.maxTuples");
  }

  
  /**
   * Gets the offset per partition.
   *
   * @return the offset per partition
   */
  public int getOffsetPerPartition() {
    return getInt("partitionner.offsetPerPartition");
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
   * Checks if is disk buffering enabled.
   *
   * @return true, if checks if is disk buffering enabled
   */
  public boolean isDiskBufferingEnabled() {
    return getBoolean("memoryBuffer.enabled");
  }
  
  
  
  /**
   * Check property.
   *
   * @param name the name
   */
  private void checkProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new RuntimeException("Failed to find required configuration option '" + name + "'.");
    }
  }

  /**
   * Gets the string.
   *
   * @param name the name
   * @return the string
   */
  private String getString(String name) {
    checkProperty(name);
    return properties.getString(name);
  }

  /**
   * Gets the int.
   *
   * @param name the name
   * @return the int
   */
  private int getInt(String name) {
    checkProperty(name);
    return properties.getInt(name);
  }

  /**
   * Gets the boolean.
   *
   * @param name the name
   * @return the boolean
   */
  private boolean getBoolean(String name) {
    return properties.getBoolean(name);
  }

}
