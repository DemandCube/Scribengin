package com.neverwinterdp.scribengin.registry.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DefaultWatcher implements Watcher {
  public void process(WatchedEvent event) {
    System.out.println("on event: " + event.getPath() + " - " + event.getType() + " - " + event.getState());
  }
}
