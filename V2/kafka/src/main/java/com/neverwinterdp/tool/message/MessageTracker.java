package com.neverwinterdp.tool.message;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MessageTracker {
  TreeMap<Integer, PartitionMessageTracker> partitions = new TreeMap<>() ;

  public void log(Message message) {
    log(message.getPartition(), message.getTrackId());
  }
  
  public void log(int partition, int trackId) {
    PartitionMessageTracker partitionTracker = getPartitionMessageTracker(partition) ;
    partitionTracker.log(trackId);
  }
  
  PartitionMessageTracker getPartitionMessageTracker(int partition) {
    PartitionMessageTracker partitionTracker = partitions.get(partition) ;
    if(partitionTracker != null) return partitionTracker; 
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
  }
}
