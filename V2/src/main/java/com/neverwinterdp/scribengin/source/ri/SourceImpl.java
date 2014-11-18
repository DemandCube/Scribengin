package com.neverwinterdp.scribengin.source.ri;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

/**
 * @author Tuan Nguyen
 */
public class SourceImpl implements Source {
  private SourceDescriptor config ;
  private Map<Integer,SourceStreamImpl> streams = new LinkedHashMap<Integer, SourceStreamImpl>();
  
  public SourceImpl(SourceDescriptor config) {
    this.config = config ;
  }
  
  public SourceImpl(SourceDescriptor config, int numOfStream, int streamSize) {
    this(config) ;
    for(int i = 0; i < numOfStream; i++) {
      SourceStreamImpl stream = new SourceStreamImpl(i, generate(streamSize, 32)) ;
      streams.put(i, stream) ;
    }
  }
  
  public SourceDescriptor getSourceDescriptor() { return config; }

  public SourceStream   getSourceStream(int id) { return streams.get(id) ; }
  
  public SourceStream   getSourceStream(SourceStreamDescriptor descriptor) { 
    return streams.get(descriptor.getId()) ; 
  }
  
  public SourceStream[] getSourceStreams() {
    SourceStream[] array = new SourceStream[streams.size()];
    return streams.values().toArray(array);
  }
  
  static public List<Record> generate(int size, int dataSize) {
    List<Record> records = new ArrayList<Record>() ;
    for(int i = 0; i < size; i++) {
      records.add(new Record()) ;
    }
    return records ;
  }
}
