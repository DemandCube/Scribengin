package com.neverwinterdp.scribengin.kafka.sink;

import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

//TODO: Allow the writer write to the assigned partition and configure the send time out
public class KafkaSinkStreamWriter implements SinkStreamWriter {
  private SinkStreamDescriptor descriptor;
  private KafkaWriter kafkaWriter ;
  private String topic;
  
  public KafkaSinkStreamWriter(SinkStreamDescriptor descriptor) {
    this.descriptor = descriptor;
    this.kafkaWriter = new KafkaWriter(descriptor.attribute("name"), descriptor.attribute("broker.list")) ;
    this.topic = descriptor.attribute("topic");
  }
  
  @Override
  public void append(Record record) throws Exception {
    kafkaWriter.send(topic, record, 5000);
  }


  @Override
  public void close() throws Exception {
    kafkaWriter.close();
  }

  @Override
  public void rollback() throws Exception {

  }

  @Override
  public void commit() throws Exception {

  }

  @Override
  public void prepareCommit() {

  }

  @Override
  public void completeCommit() {

  }
}
