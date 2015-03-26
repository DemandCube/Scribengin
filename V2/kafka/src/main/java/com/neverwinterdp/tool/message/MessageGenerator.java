package com.neverwinterdp.tool.message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.util.JSONSerializer;

public interface MessageGenerator {
  
  public byte[] nextMessage(int partition, int messageSize);
  
  
  static public class DefaultMessageGenerator implements MessageGenerator {
    private Map<Integer, AtomicInteger> idTrackers = new HashMap<>() ;
    
    @Override
    public byte[] nextMessage(int partition, int messageSize) {
      AtomicInteger idTracker = getIdTracker(partition) ;
      Message message = new Message(partition, idTracker.incrementAndGet(), messageSize) ;
      return JSONSerializer.INSTANCE.toBytes(message) ;
    }
    
    AtomicInteger getIdTracker(int partition) {
      AtomicInteger idTracker = idTrackers.get(partition) ;
      if(idTracker != null) return idTracker; 
      synchronized(idTrackers) {
        idTracker = idTrackers.get(partition) ;
        if(idTracker != null) return idTracker;
        idTracker = new AtomicInteger() ;
        idTrackers.put(partition, idTracker) ;
        return  idTracker;
      }
    }
  };
}
