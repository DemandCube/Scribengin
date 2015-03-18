package com.neverwinterdp.kafka.tool.messagegenerator;

public interface KafkaMessageGenerator {
  public byte[] nextMessage(int partition, int messageSize);
}
