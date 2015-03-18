package com.neverwinterdp.scribengin.storage.sink.partitioner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.LongMath.divide;
import static java.math.RoundingMode.UP;

/**
 * The Class OffsetPartitioner.
 */
public class OffsetPartitioner implements SinkPartitioner {


  /** The offset per partition. */
  private int offsetPerPartition;

  private StringBuilder builder = new StringBuilder();

  /**
   * The Constructor.
   * 
   * @param offsetPerPartition the offset per partition
   * @param localTmpDir the local tmp dir
   * @param bucketName the bucket name
   * @param topic the topic
   * @param kafkaPartition the kafka partition
   */
  public OffsetPartitioner(int offsetPerPartition) {
    this.offsetPerPartition = offsetPerPartition;

  }

  /**
   * Gets the log file path.
   * 
   * @param offset the offset
   * @return the log file path
   */
  private String getLogFilePath() {
    builder.setLength(0);
    return builder.toString();
  }

  /**
   * Gets the log file base name.
   * 
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @return the log file basename
   */
  private String getLogFileBaseName(long startOffset, long endOffset) {
    builder.setLength(0);

    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
    builder.append(folder);
    builder.append('/');
    builder.append(startOffset);
    builder.append('_');
    builder.append(endOffset);

    return builder.toString();
  }

  /*
   * (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner#getPartition()
   */
  @Override
  public String getPartition() {
    return getLogFilePath();
  }

  /*
   * (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner#getPartition(long,
   * long)
   */
  @Override
  public String getPartition(long startOffset, long endOffset) {

    checkArgument(startOffset < endOffset);
    // ensure that they are in the same folder
    checkArgument(startOffset / offsetPerPartition == endOffset / offsetPerPartition);
    return getLogFilePath() + getLogFileBaseName(startOffset, endOffset);
  }
}