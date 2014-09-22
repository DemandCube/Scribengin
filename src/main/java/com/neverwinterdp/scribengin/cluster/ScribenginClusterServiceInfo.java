package com.neverwinterdp.scribengin.cluster;

import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.service.ServiceInfo;
/**
 * @author Richard Duarte
 */
public class ScribenginClusterServiceInfo extends ServiceInfo{
  
  @Inject @Named("scribengin:topics")
  private String topics;
  
  @Inject @Named("scribengin:kafkaHost")
  private String kafkaHost = "localhost";
  
  @Inject @Named("scribengin:kafkaPort")
  private int kafkaPort = 9091;
  
  @Inject @Named("scribengin:workerCheckTimerInterval")
  private int workerCheckTimerInterval = 5000;
  
  public List<String> getTopics(){
    String[] s = topics.split(",");
    return Arrays.asList(s); 
  }
  
  public String getKafkaHost(){return kafkaHost; }
  public int getKafkaPort(){return kafkaPort; }
  public int getWorkerCheckTimerInterval(){return workerCheckTimerInterval; }
}