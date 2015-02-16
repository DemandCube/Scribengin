package com.neverwinterdp.scribengin.inMemory.source;

import java.util.ArrayList;

import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

public class TestSource implements Source{
  
  ArrayList<TestSourceStream> streams;
  protected SourceDescriptor sourceDesc;
  
  public TestSource(SourceDescriptor desc){
    this.sourceDesc = desc;
    streams = new ArrayList<TestSourceStream>();
    for(int i=0; i<5; i++){
      SourceStreamDescriptor x = new SourceStreamDescriptor();
      x.setType("Test");
      x.setId(i);
      streams.add(new TestSourceStream(x));
    }
  }
  
  @Override
  public SourceDescriptor getDescriptor() {
    return this.sourceDesc;
  }

  @Override
  public SourceStream getStream(int id) {
    return streams.get(id);
  }

  @Override
  public SourceStream getStream(SourceStreamDescriptor descriptor) {
    return streams.get(descriptor.getId());
  }

  @Override
  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[streams.size()];
    return (SourceStream[]) streams.toArray(array);
    
  }

  @Override
  public void close() throws Exception {
    
  }

}
