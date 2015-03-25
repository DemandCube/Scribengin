package com.neverwinterdp.tool.message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.util.JSONSerializer;

public interface MessageGenerator {
  static public MessageGenerator DEFAULT_MESSAGE_GENERATOR = new MessageGenerator() {
    private Map<Integer, AtomicInteger> idTrackers = new HashMap<>() ;
    
    @Override
    public byte[] nextMessage(int partition, int messageSize) {
      AtomicInteger idTracker = getIdTracker(partition) ;
      Message message = new Message(partition, idTracker.getAndIncrement(), messageSize) ;
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
  
  public byte[] nextMessage(int partition, int messageSize);
}
