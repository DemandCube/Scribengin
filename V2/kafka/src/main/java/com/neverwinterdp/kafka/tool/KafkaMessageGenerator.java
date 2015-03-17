package com.neverwinterdp.kafka.tool;

public class KafkaMessageGenerator {
  public byte[] nextMessage(int partition, int messageSize) {
    return new byte[messageSize] ;
  }
}
