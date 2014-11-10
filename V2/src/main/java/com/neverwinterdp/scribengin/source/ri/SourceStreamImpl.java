package com.neverwinterdp.scribengin.source.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class SourceStreamImpl implements SourceStream {
  private SourceStreamDescriptor descriptor ;
  private List<Record> records ;
  
  public SourceStreamImpl(int id) {
    this.records = new ArrayList<Record>() ;
    this.descriptor = new SourceStreamDescriptor() ;
    this.descriptor.setId(id);
    this.descriptor.setLocation("inmemory:" + id);
  }
  
  public SourceStreamImpl(int id, List<Record> records) {
    this(id) ;
    this.records = records ;
  }
  
  public SourceStreamDescriptor getDescriptor() { return descriptor ; }
  
  @Override
  public SourceStreamReader getReader(String name) {
    return new SourceStreamReaderImpl(name, this, records) ;
  }

}
