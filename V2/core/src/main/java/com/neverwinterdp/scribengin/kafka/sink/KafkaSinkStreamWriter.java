package com.neverwinterdp.scribengin.kafka.sink;

import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

//TODO: Allow the writer write to the assigned partition and configure the send time out
public class KafkaSinkStreamWriter implements SinkStreamWriter {
  private StreamDescriptor descriptor;
  private DefaultKafkaWriter defaultKafkaWriter ;
  private String topic;
  
  public KafkaSinkStreamWriter(StreamDescriptor descriptor) {
    this.descriptor = descriptor;
    this.defaultKafkaWriter = new DefaultKafkaWriter(descriptor.attribute("name"), descriptor.attribute("broker.list")) ;
    this.topic = descriptor.attribute("topic");
  }
  
  @Override
  public void append(Record record) throws Exception {
    defaultKafkaWriter.send(topic, record, 5000);
  }


  @Override
  public void close() throws Exception {
    defaultKafkaWriter.close();
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
