package com.neverwinterdp.tool.message;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MessageTracker {
  private TreeMap<Integer, PartitionMessageTracker> partitions = new TreeMap<>() ;
  
  public int getLogCount() { 
    int logCount  = 0;
    for(PartitionMessageTracker sel : partitions.values()) {
      logCount +=  sel.getLogCount();
    }
    return logCount ; 
  }
  
  public int getDuplicatedCount() {
    int duplicatedCount  = 0;
    for(PartitionMessageTracker sel : partitions.values()) {
      duplicatedCount +=  sel.getDuplicatedCount();
    }
    return duplicatedCount;
  }
  
  public boolean isInSequence() {
    for(PartitionMessageTracker sel : partitions.values()) {
      if(!sel.isInSequence()) return false;
    }
    return true;
  }
  
  public void log(Message message) {
    log(message.getPartition(), message.getTrackId());
  }
  
  public void log(int partition, int trackId) {
    PartitionMessageTracker partitionTracker = getPartitionMessageTracker(partition, true) ;
    partitionTracker.log(trackId);
  }
  
  public PartitionMessageTracker getPartitionMessageTracker(int partition) {
    return partitions.get(partition) ;
  }
  
  PartitionMessageTracker getPartitionMessageTracker(int partition, boolean create) {
    PartitionMessageTracker partitionTracker = partitions.get(partition) ;
    if(partitionTracker != null) return partitionTracker; 
    if(!create) return null ;
    synchronized(partitions) {
      partitionTracker = partitions.get(partition) ;
      if(partitionTracker != null) return partitionTracker;
      partitionTracker = new PartitionMessageTracker(partition) ;
      partitions.put(partition, partitionTracker) ;
      return  partitionTracker;
    }
  }
  
  public void optimize() {
    for(PartitionMessageTracker sel : partitions.values()) {
      sel.optimize();
    }
  }
  
  public void dump(Appendable out) throws IOException {
    out.append("\nMessage Tracker: \n\n");
    for(Map.Entry<Integer, PartitionMessageTracker> entry : partitions.entrySet()) {
      int partition = entry.getKey() ;
      PartitionMessageTracker partitionTracker = entry.getValue() ;
      partitionTracker.dump(out, "Tracking message for the partition " + partition);
    }
    out.append("\nLog Count: " + getLogCount() + "\n");
  }
}
