package com.neverwinterdp.scribengin.scribeconsumer;

import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.scribengin.hostport.HostPort;

public class ScribeConsumerConfig {
  //ScribeConsumer config
  public String PRE_COMMIT_PATH_PREFIX = "/tmp";
  public  String COMMIT_PATH_PREFIX = "/committed";
  public String topic = "scribe";
  public int partition = 0;
  public long commitCheckPointInterval = 5000; // ms
  public String hdfsPath = null;
  public List<HostPort> brokerList; // list of (host:port)s
  
  //Set by cleanStart() method
  public boolean cleanStart = false;
  
  public String date_partitioner = null;
  
  //Yarn config parameters
  public String appname = "ScribeConsumer";
  public String scribenginJarPath = "/scribengin-1.0-SNAPSHOT.jar";
  public String appMasterClassName = com.neverwinterdp.scribengin.yarn.ScribenginAM.class.getName();
  //public String libHadoopPath= "/usr/lib/hadoop/lib/native/libhadoop.so";
  public String yarnSiteXml = "/etc/hadoop/conf/yarn-site.xml";
  public String defaultFs = "hdfs://127.0.0.1";
  public int containerMem = 300;
  public int applicationMasterMem = 300;
  
  public ScribeConsumerConfig(){
    this.brokerList = new LinkedList<HostPort>();
    this.brokerList.add(new HostPort("127.0.0.1","9092"));
  }
  
  /**
   * Copy constructor
   * @param c
   */
  public ScribeConsumerConfig(ScribeConsumerConfig c){
    this.PRE_COMMIT_PATH_PREFIX = c.PRE_COMMIT_PATH_PREFIX;
    this.COMMIT_PATH_PREFIX = c.COMMIT_PATH_PREFIX;
    this.topic = c.topic;
    this.partition = c.partition;
    this.commitCheckPointInterval = c.commitCheckPointInterval; // ms
    this.hdfsPath = c.hdfsPath;
    this.brokerList = new LinkedList<HostPort>();
    this.brokerList.addAll(c.brokerList);
    
    //Set by cleanStart() method
    this.cleanStart = c.cleanStart;
    
    this.date_partitioner = c.date_partitioner;
    
    //Yarn config parameters
    this.appname = c.appname;
    this.scribenginJarPath = c.scribenginJarPath;
    this.appMasterClassName = c.appMasterClassName;
    //public String libHadoopPath= "/usr/lib/hadoop/lib/native/libhadoop.so";
    this.yarnSiteXml = c.yarnSiteXml;
    this.defaultFs = c.defaultFs;
    this.containerMem = c.containerMem;
    this.applicationMasterMem = c.applicationMasterMem;
  }
  
  public ScribeConsumerConfig(String topic){
    this();
    this.topic = topic;
  }
  
  public String getBrokerListAsString(){
    String brokers = "";
    for(HostPort x: this.brokerList){
      brokers += x.getHost()+":"+x.getPort()+",";
    }
    return brokers.substring(0, brokers.length() - 1);
  }
  
  public List<String> getBrokerListAsListOfStrings(){
    List<String> brokers = new LinkedList<String>(); 
    for(HostPort x: this.brokerList){
      brokers.add(x.getHost()+":"+x.getPort()+",");
    }
    return brokers;
  }
  
  
  /**
   * Configures the brokerList, everything else is defaults
   * @param brokerList List of strings pointing to kafka, expecting to be in "[hostname/ip]:[port]" format
   */
  public ScribeConsumerConfig(List<String> brokerList){
    List<HostPort> x = new LinkedList<HostPort>();
    for(String s: brokerList){
      String[] split = s.split(":");
      x.add(new HostPort(split[0],split[1]));
    }
    this.brokerList = x;
  }
}
