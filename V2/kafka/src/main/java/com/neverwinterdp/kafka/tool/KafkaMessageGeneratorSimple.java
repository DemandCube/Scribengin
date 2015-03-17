package com.neverwinterdp.kafka.tool;

public class KafkaMessageGeneratorSimple implements KafkaMessageGenerator{
  public byte[] nextMessage(int partition, int messageSize) {
    return new byte[messageSize] ;
  }
}
