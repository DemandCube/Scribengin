package com.neverwinterdp.scribengin.source.kafka;

import java.util.Collection;
import java.util.Set;

import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.util.HostPort;

public class KafkaSourceStreamDescriptor extends SourceStreamDescriptor {

  private String topic;


  private Collection<HostPort> brokers;

  public KafkaSourceStreamDescriptor(String topic, String partition, Collection<HostPort> brokers) {
    this.topic = topic;
    this.id = Integer.parseInt(partition);
    this.brokers = brokers;
  }

  public Collection<HostPort> getBrokers() {
    return brokers;
  }

  public String getTopic() {
    return topic;
  }

  public void setBrokers(Set<HostPort> brokers) {
    this.brokers = brokers;
  }


}
