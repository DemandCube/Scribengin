package com.neverwinterdp.scribengin.ScribeConsumerManager;

import java.util.List;

import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;

/**
 * Should be able to start and monitor launched servers
 * @author Richard Duarte
 *
 */
public abstract class AbstractScribeConsumerManager {
  /**
   * Starts ScribeConsumer
   * @return True on successfully starting consumer, otherwise false
   */
  public abstract boolean startNewConsumer(ScribeConsumerConfig c);
  
  public abstract boolean startNewConsumers(ScribeConsumerConfig c, List<String> topics);
  
  /**
   * Check the state of a launched server
   * If server has died, relaunch
   */
  public abstract void monitorConsumers(); 
  
  public abstract boolean shutdownConsumers();
  
  public abstract int getNumConsumers();
}
