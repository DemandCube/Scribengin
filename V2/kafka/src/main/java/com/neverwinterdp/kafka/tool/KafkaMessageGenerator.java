package com.neverwinterdp.kafka.tool;

public interface KafkaMessageGenerator {
  public byte[] nextMessage(int partition, int messageSize);
}
