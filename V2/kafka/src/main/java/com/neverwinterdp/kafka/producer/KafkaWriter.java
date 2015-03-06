package com.neverwinterdp.kafka.producer;

import java.util.List;

import org.apache.kafka.clients.producer.Callback;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public interface KafkaWriter {

  public void reconnect() ;

  public void send(String topic, String data, long timeout) throws Exception ;
  
  public void send(String topic, String key, String data, long timeout) throws Exception ;
  
  public void send(String topic, int partition, String key, String data, long timeout) throws Exception ;
  
  public void send(String topic, int partition, String key, String data, Callback callback, long timeout) throws Exception ;
  
  public <T> void send(String topic, T obj, long timeout) throws Exception ;

  public void send(String topic, List<String> dataHolder, long timeout) throws Exception ;

  public void close() throws Exception ;
}