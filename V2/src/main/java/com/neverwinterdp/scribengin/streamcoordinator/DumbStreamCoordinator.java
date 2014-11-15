package com.neverwinterdp.scribengin.streamcoordinator;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.scribe.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.stream.StreamImpl;
import com.neverwinterdp.scribengin.task.DumbTask;

public class DumbStreamCoordinator implements StreamCoordinator{

  private int numScribes;
  
  public DumbStreamCoordinator(){
    this(10);
  }
  
  public DumbStreamCoordinator(int numScribes){
    this.numScribes = numScribes;
  }
  
  @Override
  public Scribe[] allocateStreams() {
    
    Scribe[] s = new Scribe[numScribes];
    
    for(int i = 0; i < numScribes; i++){
      s[i] = new ScribeImpl(new StreamImpl(new UUIDSourceStream(), 
          new InMemorySinkStream(new DumbSinkPartitioner()), 
          new InMemorySinkStream(new DumbSinkPartitioner()), 
          new DumbTask()));
    }
    
    return s;
  }

}
