package com.neverwinterdp.scribengin.stream.sink.partitioner;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

/**
 * The Class OffsetPartitioner.
 */
public class OffsetPartitioner implements SinkPartitioner {

  /** The topic. */
  private String topic;
  
  /** The kafka partition. */
  private int kafkaPartition;
  
  /** The offset per partition. */
  private int offsetPerPartition;
  


  
  /**
   * The Constructor.
   *
   * @param offsetPerPartition the offset per partition
   * @param localTmpDir the local tmp dir
   * @param bucketName the bucket name
   * @param topic the topic
   * @param kafkaPartition the kafka partition
   */
  public OffsetPartitioner(int offsetPerPartition, String topic, int kafkaPartition) {
    this.offsetPerPartition = offsetPerPartition;
    this.topic = topic;
    this.kafkaPartition = kafkaPartition;

  }

  /**
   * Gets the log file path.
   *
   * @param offset the offset
   * @return the log file path
   */
  private String getLogFilePath(long offset) {
    int partition = (int) ((offset / offsetPerPartition) * offsetPerPartition);
    ArrayList<String> pathElements = new ArrayList<String>();
    pathElements.add(topic);
    pathElements.add(Integer.toString(kafkaPartition));
    pathElements.add("offset=" + partition);

    return StringUtils.join(pathElements, "/");
  }

  /**
   * Gets the log file base name.
   *
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @return the log file basename
   */
  private String getLogFileBaseName(long startOffset, long endOffset) {
    ArrayList<String> basenameElements = new ArrayList<String>();
    basenameElements.add(Long.toString(startOffset));
    basenameElements.add(Long.toString(endOffset));
    return StringUtils.join(basenameElements, "_");
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner#getPartition()
   */
  @Override
  public String getPartition() {
    return "";
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner#getPartition(long, long)
   */
  @Override
  public String getPartition(long startOffset, long endOffset) {
    return getLogFilePath(startOffset) + "/" + getLogFileBaseName(startOffset, endOffset);
  }

}
