package com.neverwinterdp.kafka.producer;

import org.apache.kafka.clients.producer.Callback;

public class KafkaMessageSendTool implements Runnable {

  private AckKafkaWriter writer;
  private final int TIMEOUT = 99999;
  private Callback callback;
  private String message;
  int counter = 0;
  private FixedMessageSizeGenerator messageGenerator;
  private String topic;

  public KafkaMessageSendTool(String topic, String brokerURL, FixedMessageSizeGenerator messageGenerator,
      Callback callback) {
    this.topic=topic;
    this.callback = callback;
    this.messageGenerator = messageGenerator;
    writer = new AckKafkaWriter(topic, brokerURL);
  }

  @Override
  public void run() {
    message = messageGenerator.next();
 //   System.out.println("writing " + message.substring(0, 3));
    try {
      writer.send(topic, message.getBytes(), message.getBytes(), callback, TIMEOUT);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}