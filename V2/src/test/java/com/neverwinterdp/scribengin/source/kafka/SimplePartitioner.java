package com.neverwinterdp.scribengin.source.kafka;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

  public SimplePartitioner(VerifiableProperties props) {

  }

  public int partition(Object key, int numPartitions) {

    return new Random().nextInt(numPartitions);
  }

}
