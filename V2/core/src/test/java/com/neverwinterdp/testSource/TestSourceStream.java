package com.neverwinterdp.testSource;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class TestSourceStream implements SourceStream{

  protected SourceStreamDescriptor sourceDesc;
  protected Map<String, TestSourceReader> streams;
  
  public TestSourceStream(SourceStreamDescriptor desc){
    this.sourceDesc = desc;
    streams = new HashMap<String, TestSourceReader>();
    
    streams.put("0", new TestSourceReader());
    streams.put("1", new TestSourceReader());
  }
  
  @Override
  public SourceStreamDescriptor getDescriptor() {
    return this.sourceDesc;
  }

  @Override
  public SourceStreamReader getReader(String name) throws Exception {
    System.err.println("Getting reader: "+name);
    for (Map.Entry<String, TestSourceReader> entry : streams.entrySet()) {
      System.err.println("KEY: "+entry.getKey());
      TestSourceReader value = entry.getValue();
      // ...
    }
    System.err.println("");
    
    
    
    if(streams.size() > 0 ){
      SourceStreamReader stream = streams.get(name);
      if(stream != null) return stream ;
    }
    TestSourceReader newStream= new TestSourceReader() ;
    streams.put(Integer.toString(streams.size()), newStream) ;
    return newStream;
    
  }

}
