package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.Registry;

public class WaitingOrderNodeEventListener extends WaitingNodeEventListener {
  public WaitingOrderNodeEventListener(Registry registry) {
    super(registry);
  }
  
  synchronized protected void onDetectNodeEvent(NodeWatcher watcher, NodeEvent event) {
    NodeWatcher waitingWatcher = watcherQueue.getFirst() ;
    if(waitingWatcher == watcher) {
      long time = System.currentTimeMillis() ;
      watcherQueue.removeFirst();
      detectNodeEventCount++;
      eventLogs.add(new NodeEventLog(time - estimateLastDetectEventTime, event, watcher)) ;
      estimateLastDetectEventTime = time ;
      notifyAll();
    }
  }
}