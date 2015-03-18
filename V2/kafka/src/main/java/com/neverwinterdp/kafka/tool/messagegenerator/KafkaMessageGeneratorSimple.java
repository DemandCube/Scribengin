package com.neverwinterdp.kafka.tool.messagegenerator;

public class KafkaMessageGeneratorSimple implements KafkaMessageGenerator{
  public byte[] nextMessage(int partition, int messageSize) {
    return new byte[messageSize] ;
  }
}
