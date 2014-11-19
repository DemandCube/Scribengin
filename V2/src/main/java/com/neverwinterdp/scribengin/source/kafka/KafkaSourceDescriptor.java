package com.neverwinterdp.scribengin.source.kafka;

import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.util.HostPort;

public class KafkaSourceDescriptor extends SourceDescriptor {

  //location is zookeeperURL
  private HostPort hostPort;

  public HostPort getHostPort() {
   //TODO get host port from location
    return hostPort;
  }

}
