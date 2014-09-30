package com.neverwinterdp.scribengin.ScribeConsumerManager;

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
  
  /**
   * Check the state of a launched server
   * If server has died, relaunch
   */
  public abstract void monitorConsumers(); 
  
  public abstract boolean shutdownConsumers();
}
