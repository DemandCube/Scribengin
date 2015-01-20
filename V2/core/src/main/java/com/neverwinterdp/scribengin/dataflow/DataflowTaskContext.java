package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceFactory;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class DataflowTaskContext {
  
  private SourceContext sourceContext ;
  private Map<String, SinkContext> sinkContexts = new HashMap<String, SinkContext>();
  
  public DataflowTaskContext(DataflowContainer container, DataflowTaskDescriptor descriptor) throws Exception {
    this.sourceContext = new SourceContext(container.getSourceFactory(), descriptor.getSourceStreamDescriptor());
    Iterator<Map.Entry<String, SinkStreamDescriptor>> i = descriptor.getSinkStreamDescriptors().entrySet().iterator() ;
    while(i.hasNext()) {
      Map.Entry<String, SinkStreamDescriptor> entry = i.next();
      SinkContext context = new SinkContext(container.getSinkFactory(), entry.getValue());
      sinkContexts.put(entry.getKey(), context) ;
    }
  }
  
  public SourceStreamReader getSourceStreamReader() {
    return sourceContext.assignedSourceStreamReader;
  }
  
  public void write(Record record) throws Exception {
    SinkContext sinkContext = sinkContexts.get("default") ;
    sinkContext.assignedSinkStreamWriter.append(record);
  }
  
  public void write(String sinkName, Record record) throws Exception {
    SinkContext sinkContext = sinkContexts.get(sinkName) ;
    sinkContext.assignedSinkStreamWriter.append(record);
  }
  
  public void commit() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while(i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.commit();
    }
    sourceContext.commit();
  }
  
  public void rollback() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while(i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.rollback();;
    }
    sourceContext.rollback();
  }
  
  public void close() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while(i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.close();;
    }
    sourceContext.close();
  }
  
  static public class  SourceContext {
    private Source source ;
    private SourceStream assignedSourceStream ;
    private SourceStreamReader assignedSourceStreamReader;
  
    public SourceContext(SourceFactory factory, SourceStreamDescriptor streamDescriptor) throws Exception {
      this.source = factory.create(streamDescriptor) ;
      this.assignedSourceStream = source.getStream(streamDescriptor.getId());
      this.assignedSourceStreamReader = assignedSourceStream.getReader("DataflowTask");
    }
    
    public void commit() throws Exception {
      assignedSourceStreamReader.commit();
    }
    
    public void rollback() throws Exception {
      assignedSourceStreamReader.rollback();
    }
    
    public void close() throws Exception {
      assignedSourceStreamReader.close();
    }
  }
  
  static public class  SinkContext {
    private Sink sink ;
    private SinkStream assignedSinkStream ;
    private SinkStreamWriter assignedSinkStreamWriter;
  
    
    public SinkContext(SinkFactory factory, SinkStreamDescriptor streamDescriptor) throws Exception {
      this.sink = factory.create(streamDescriptor);
      this.assignedSinkStream = sink.getStream(streamDescriptor);
      this.assignedSinkStreamWriter = this.assignedSinkStream.getWriter();
    }
    
    public void commit() throws Exception {
      assignedSinkStreamWriter.commit();
    }
    
    public void rollback() throws Exception {
      assignedSinkStreamWriter.rollback();
    }
    
    public void close() throws Exception {
      assignedSinkStreamWriter.close();
    }
  }
}