package com.neverwinterdp.kafka.tool;

public interface KafkaMessageGenerator {
  static public KafkaMessageGenerator DEFAULT_MESSAGE_GENERATOR = new KafkaMessageGenerator() {

    @Override
    public byte[] nextMessage(int partition, int messageSize) {
      return new byte[messageSize] ;
    }
  };
  
  public byte[] nextMessage(int partition, int messageSize);
}
