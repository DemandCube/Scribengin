package com.neverwinterdp.scribengin.scribengin;

import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.config.ScribeWorkerConfig;
import com.neverwinterdp.scribengin.scribeworker.ScribeWorker;

public class Scribengin {
  
  private class scribeTuple{ 
    public ScribeWorker worker; 
    public ScribeWorkerConfig config; 
    public scribeTuple(ScribeWorker worker, ScribeWorkerConfig config) { 
      this.worker = worker; 
      this.config = config; 
    } 
  } 
  
  private static final Logger log = Logger.getLogger(Scribengin.class.getName());
  private List<String> topics;
  private List<scribeTuple> scribes;
  private String kafkaHost;
  private int kafkaPort;
  private int workerCheckTimerInterval;
  private Timer workerCheckTimer;
  

  public Scribengin(List<String> topics, String kafkaHost, int kafkaPort){
    this(topics, kafkaHost, kafkaPort, 5000);
  }
  
  public Scribengin(List<String> topics, String kafkaHost, int kafkaPort, int workerCheckTimerInterval){
    this.topics    = topics;
    this.kafkaHost = kafkaHost;
    this.kafkaPort = kafkaPort;
    this.workerCheckTimerInterval = workerCheckTimerInterval;
    this.workerCheckTimer = new Timer();
    this.scribes = new LinkedList<scribeTuple>();
    
    for(String s: topics){
      log.info("Scribengin is starting topic: "+s);
      ScribeWorkerConfig c = new ScribeWorkerConfig(this.kafkaHost, this.kafkaPort, s);
      scribes.add(new scribeTuple(new ScribeWorker(c),c));
    }
  }
  
  public void start(){
    log.info("Starting Scribengin");
    for(scribeTuple s: scribes){
      log.info("Starting worker");
      s.worker.start();
    }
    workerCheckTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        checkScribeWorkerStatus();
      }
    }, workerCheckTimerInterval);
  }
  
  public void stop(){
    log.info("Stopping Scribengin");
    workerCheckTimer.cancel();
    for(scribeTuple s: scribes){
      s.worker.stop();
    }
  }
  
  private void checkScribeWorkerStatus(){
    for(int i=0; i< scribes.size(); i++){
      Thread.State state = scribes.get(i).worker.getState();
      
      //If thread has been terminated, restart it
      if(state == Thread.State.TERMINATED){
        log.info("Terminated thread found.  Restarting worker for topic: "+scribes.get(i).config.topic);
        
        //Make sure its completely stopped
        try{
          scribes.get(i).worker.stop();
        } catch(Exception e){}
        
        ScribeWorkerConfig c = scribes.get(i).config;
        ScribeWorker sw = new ScribeWorker(c);
        sw.start();
        scribes.add(new scribeTuple(sw,c));
        
        scribes.remove(i);
        log.info("New worker created!");
      }
    }
  }

  public List<String> getTopics() {
    return topics;
  }
  
  private void killWorker(int i){
    log.info("TESTING ONLY - Killing worker "+Integer.toString(i)+" TOPIC: "+scribes.get(i).config.topic);
    scribes.get(i).worker.stop();
  }
  
}
