package com.neverwinterdp.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.base.Stopwatch;

class KafkaMessageSendTool implements Runnable {
  /**
   * 
   */
  private String topic;
  private int maxNumOfMessages = 10000;
  int messageSize = 1024;
  private long waitBeforeClose = 10000;
  private int numOfSentMessages = 0;
  private int numOfFailedAck = 0;
  private boolean exit = false;
  Stopwatch stopwatch = Stopwatch.createUnstarted();
  private String kafkaBrokersURL;

  public KafkaMessageSendTool(String kafkaBrokersURL, String topic, int maxNumOfMessages, int messageSize) {
    this.kafkaBrokersURL= kafkaBrokersURL;
    this.topic = topic;
    this.maxNumOfMessages = maxNumOfMessages;
    this.messageSize = messageSize;
  }

  public int getNumOfSentMessages() {
    return this.numOfSentMessages;
  }

  public int getNumOfFailedAck() {
    return this.numOfFailedAck;
  }

  @Override
  public void run() {
    try {
      runSend();
    } catch (Exception e) {
      e.printStackTrace();
    }
    notifyTermination();
  }

  void runSend() throws Exception {
    stopwatch.start();
    Map<String, String> kafkaProps = new HashMap<String, String>();
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("retry.backoff.ms", "100");
    kafkaProps.put("queue.buffering.max.ms", "1000");
    kafkaProps.put("queue.buffering.max.messages", "15000");
    //kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "100");
    kafkaProps.put("producer.type", "sync");
    //new config:
    kafkaProps.put("acks", "all");

    AckKafkaWriter writer = new AckKafkaWriter("KafkaMessageSendTool", kafkaProps, kafkaBrokersURL);
    FailedAckReportCallback failedAckReportCallback = new FailedAckReportCallback();
    while (!exit && numOfSentMessages < maxNumOfMessages) {
      //Use this send to print out more detail about the message lost
      byte[] key = ("key-" + numOfSentMessages).getBytes();
      byte[] message = new byte[messageSize];
      writer.send(topic, key, message, failedAckReportCallback, 10000);
      numOfSentMessages++;
    }
    writer.waitAndClose(waitBeforeClose);
    numOfFailedAck = failedAckReportCallback.getCount();
    stopwatch.stop();
  }

  synchronized public void notifyTermination() {
    notify();
  }

  synchronized public void waitTermination(long timeout) throws InterruptedException {
    wait(timeout);
    exit = true;
  }
  
  class FailedAckReportCallback implements Callback {
    private int count;

    public int getCount() {
      return count;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null)
        count++;
    }
  }
}