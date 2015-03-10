package com.neverwinterdp.kafka.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class AckKafkaWriter {
  static private Logger LOGGER = LoggerFactory.getLogger(AckKafkaWriter.class);
  
  private String name;
  private Properties kafkaProperties;
  private KafkaProducer<byte[], byte[]> producer;
  private AtomicLong idTracker = new AtomicLong();
  private WaittingAckProducerRecordHolder<byte[], byte[]> waittingAckBuffer = new WaittingAckProducerRecordHolder<byte[], byte[]>();
  private ResendThread resendThread ;
  
  public String getName() { return name; }

  public void setName(String name) { this.name = name; }

  public AckKafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls);
  }

  public AckKafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    this.name = name;
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafkaBrokerUrls);
    kafkaProps.put("value.serializer", ByteArraySerializer.class.getName());
    kafkaProps.put("key.serializer",   ByteArraySerializer.class.getName());
    if (props != null) {
      kafkaProps.putAll(props);
    }
    this.kafkaProperties = kafkaProps;
    reconnect();
  }

  public void reconnect() {
    if (producer != null) producer.close();
    producer = new KafkaProducer<byte[], byte[]>(kafkaProperties);
  }
  
  public void send(String topic, int partition, String key, String message, Callback callback, long timeout) throws Exception {
    byte[] keyBytes = key.getBytes();
    byte[] messageBytes = message.getBytes();
    send(topic, partition, keyBytes, messageBytes, callback, timeout);
  }
  
  public void send(String topic, byte[] key, byte[] message, Callback callback, long timeout) throws Exception {
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, key, message);
    long id = idTracker.incrementAndGet();
    WaittingAckProducerRecord<byte[], byte[]> ackRecord = new WaittingAckProducerRecord<byte[], byte[]>(id, record, callback);
    waittingAckBuffer.add(ackRecord, timeout);
    AckCallback ackCallback = new AckCallback(id);
    producer.send(record, ackCallback);
  }
  
  public void send(String topic, int partition, byte[] key, byte[] message, Callback callback, long timeout) throws Exception {
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, partition, key, message);
    long id = idTracker.incrementAndGet();
    WaittingAckProducerRecord<byte[], byte[]> ackRecord = new WaittingAckProducerRecord<byte[], byte[]>(id, record, callback);
    waittingAckBuffer.add(ackRecord, timeout);
    AckCallback ackCallback = new AckCallback(id);
    producer.send(record, ackCallback);
  }
  
  
  
  public void triggerResendThread() {
    if(resendThread == null || !resendThread.isAlive()) {
      resendThread = new ResendThread();
      resendThread.start();
    }
  }
  
  public void waitAndClose(long timeout)  {
    try {
      int waitTime = 0;
      while(waitTime < timeout && waittingAckBuffer.size() > 0) {
        Thread.sleep(100);
        waitTime += 100;
      }
    } catch (InterruptedException e) {
    }
    producer.close(); 
  }
  
  public void close() { producer.close(); }
  
  public class ResendThread extends Thread {
    public void run() {
      System.err.println("Start resend thread");
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        return;
      }
      System.err.println("Start resend thread after 500ms");
      List<WaittingAckProducerRecord<byte[],byte[]>> needToResendRecords =  waittingAckBuffer.getNeedToResendRecords();
      while(needToResendRecords.size() > 0) {
        System.err.println("Resend " + needToResendRecords.size());
        for(WaittingAckProducerRecord<byte[], byte[]> sel : needToResendRecords) {
          AckCallback ackCallback = new AckCallback(sel.getId());
          producer.send(sel.getProducerRecord(), ackCallback);
          sel.setNeedToResend(false);
        }
        System.err.println("Resend " + needToResendRecords.size() + " done!!!!!!!!!!!!!!");
        needToResendRecords =  waittingAckBuffer.getNeedToResendRecords();
      }
    }
  }
  
  public class AckCallback implements Callback {
    private long recordId ;
    
    AckCallback(long recordId) {
      this.recordId = recordId ;
    }
    
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      try {
        if(exception != null) {
          WaittingAckProducerRecord<byte[],byte[]> ackRecord = waittingAckBuffer.onFail(recordId);
          if(ackRecord.getCallback() != null) {
            ackRecord.getCallback().onCompletion(metadata, exception);
          }
          triggerResendThread();
        } else {
          //remove the the successfully sent record
          WaittingAckProducerRecord<byte[],byte[]> ackRecord = waittingAckBuffer.onSuccess(recordId);
          if(ackRecord.getCallback() != null) {
            ackRecord.getCallback().onCompletion(metadata, exception);
          }
        }
      } catch(Exception ex) {
        LOGGER.error("Error handling ack buffer", ex);
      }
    }
  }
}