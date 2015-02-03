package com.neverwinterdp.scribengin.kafka.sink;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class SimplePartitioner implements Partitioner {
  public SimplePartitioner(VerifiableProperties props) {}

  public int partition(Object key, int numPartitions) {
    String keyStr = (String) key;
    int partition = Math.abs(keyStr.hashCode() % numPartitions);
    return partition;
  }
}
