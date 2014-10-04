package com.neverwinterdp.scribengin;

import java.util.LinkedList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.neverwinterdp.scribengin.ScribeConsumerManager.AbstractScribeConsumerManager;
import com.neverwinterdp.scribengin.ScribeConsumerManager.ClusterScribeConsumerManager;
import com.neverwinterdp.scribengin.ScribeConsumerManager.YarnScribeConsumerManager;
import com.neverwinterdp.scribengin.hostport.CustomConvertFactory;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;

public class ScribeMaster {
  private AbstractScribeConsumerManager manager;
  private List<String> topics;
  private ScribeConsumerConfig commonConf;
  
  public ScribeMaster(List<String> topics, ScribeConsumerConfig conf){
    this.topics = topics;
    this.commonConf = conf;
  }
  
  public void setScribeConsumerManager(AbstractScribeConsumerManager ascm){
    manager = ascm;
  }
  
  public void start(){
    try{
      manager.startNewConsumers(commonConf,topics) ;
    } catch(Exception e){
      e.printStackTrace();
    }
  }
  
  
  public void stop(){
    manager.shutdownConsumers();
  }
  
  public void checkOnConsumers(){
    manager.monitorConsumers();
  }
  
  public int getNumConsumers(){
    return manager.getNumConsumers();
  }
  
  public static void main(String[] args){
    ScribeMasterCommandLineArgs p = new ScribeMasterCommandLineArgs();
    
    JCommander jc = new JCommander(p);
    jc.addConverterFactory(new CustomConvertFactory());
    try{
      jc.parse(args);
    } catch (ParameterException e){
      System.err.println(e.getMessage());
      jc.usage();
      System.exit(-1);
    }
    List<String> topics = new LinkedList<String>();
    String[] split = p.topic.split(",");
    for(String x: split){
      topics.add(x);
    }
    
    ScribeConsumerConfig c = new ScribeConsumerConfig();
    c.PRE_COMMIT_PATH_PREFIX = p.preCommitPrefix;
    c.COMMIT_PATH_PREFIX = p.commitPrefix;
    c.partition = p.partition;
    c.commitCheckPointInterval = p.commitCheckPointInterval;
    c.hdfsPath = p.hdfsPath;
    c.brokerList = p.brokerList;
    
    c.cleanStart = p.cleanstart;
    
    c.appname = p.appname;
    c.scribenginJarPath = p.scribenginjar;
    c.appMasterClassName = p.appMasterClassName;
    c.containerMem = p.containerMem;
    c.applicationMasterMem = p.applicationMasterMem;
    
    
    final ScribeMaster sm = new ScribeMaster(topics, c);
    
    if(p.mode.equals("dev") || p.mode.equals("distributed")){
      sm.setScribeConsumerManager(new ClusterScribeConsumerManager());
    }
    else if(p.mode.equals("yarn")){
      sm.setScribeConsumerManager(new YarnScribeConsumerManager());
    }
    else{
      System.err.println("Invalid mode: "+p.mode);
      System.err.println("Valid modes: dev, distributed, yarn");
      System.exit(-1);
    }
    
    //This is so when we hit ctrl+c it kills app cleanly
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
       sm.stop();
      }
     });
    
    sm.start();
    
    while(true){
      sm.checkOnConsumers();
      try {
        Thread.sleep(5000);
        if(sm.getNumConsumers() < 1){
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
  }
}
