package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class DataflowTaskContext {
  private DataflowTaskReport report;
  private SourceContext sourceContext;
  private Map<String, SinkContext> sinkContexts = new HashMap<String, SinkContext>();
  private DataflowRegistry dataflowRegistry;
  private DataflowTaskDescriptor dataflowTaskDescriptor;

  public DataflowTaskContext(DataflowContainer container, DataflowTaskDescriptor descriptor, DataflowTaskReport report)
      throws Exception {
    System.err.println("creating dataflowtask context " + descriptor);
    this.sourceContext = new SourceContext(container.getSourceFactory(), descriptor.getSourceStreamDescriptor());
    Iterator<Map.Entry<String, StreamDescriptor>> i = descriptor.getSinkStreamDescriptors().entrySet().iterator();
    System.err.println(" source stream descriptor " + descriptor.getSourceStreamDescriptor());
    System.err.println(" sink stream descriptors " + descriptor.getSinkStreamDescriptors());
      while (i.hasNext()) {
        Map.Entry<String, StreamDescriptor> entry = i.next();
        System.err.println(" an entry value"+ entry.getValue());
        System.err.println("container.get sinkfactory "+ container.getSinkFactory());
        SinkContext context = new SinkContext(container.getSinkFactory(), entry.getValue());
        System.err.println("mpya "+ context);
        sinkContexts.put(entry.getKey(), context);
      }
      System.err.println("sink contexts if we ever get here!!!" + sinkContexts);
      this.report = report;
      this.dataflowTaskDescriptor = descriptor;
      this.dataflowRegistry = container.getDataflowRegistry();
  }

  public DataflowTaskReport getReport() {
    return this.report;
  }

  public SourceStreamReader getSourceStreamReader() {
    return sourceContext.assignedSourceStreamReader;
  }

  public void append(Record record) throws Exception {
    SinkContext sinkContext = sinkContexts.get("default");
    System.err.println("append to sink " + record);
    sinkContext.assignedSinkStreamWriter.append(record);
  }

  public void write(String sinkName, Record record) throws Exception {
    SinkContext sinkContext = sinkContexts.get(sinkName);
    sinkContext.assignedSinkStreamWriter.append(record);
  }

  private void prepareCommit() throws Exception {
    sourceContext.assignedSourceStreamReader.prepareCommit();
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    System.out.println("sink contexts " + sinkContexts);
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.prepareCommit();
    }
  }

  public boolean commit() throws Exception {
    //prepareCommit is a vote to make sure both sink, invalidSink, and source
    //are ready to commit data, otherwise rollback will occur
    try {
      prepareCommit();
      completeCommit();
    } catch (Exception ex) {
      rollback();
      throw ex;
    }
    report.incrCommitProcessCount();
    dataflowRegistry.dataflowTaskReport(dataflowTaskDescriptor, report);
    return false;
  }

  private void rollback() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.rollback();
    }
    sourceContext.rollback();
  }

  public void close() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.close();
      ;
    }
    sourceContext.close();
  }

  private void completeCommit() throws Exception {
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    System.err.println("nganganga " + i.hasNext());
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.completeCommit();
    }
    //The source should commit after sink commit. In the case the source or sink does not support
    //2 phases commit, it will cause the data to duplicate only, not loss
    sourceContext.assignedSourceStreamReader.completeCommit();
  }

  static public class SourceContext {
    private Source source;
    private SourceStream assignedSourceStream;
    private SourceStreamReader assignedSourceStreamReader;

    public SourceContext(SourceFactory factory, StreamDescriptor streamDescriptor) throws Exception {
      this.source = factory.create(streamDescriptor);
      this.assignedSourceStream = source.getStream(streamDescriptor.getId());
      this.assignedSourceStreamReader = assignedSourceStream.getReader("DataflowTask");
    }

    public void prepapreCommit() throws Exception {
      assignedSourceStreamReader.prepareCommit();
    }

    public void completeCommit() throws Exception {
      assignedSourceStreamReader.completeCommit();
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

  static public class SinkContext {
    private Sink sink;
    private SinkStream assignedSinkStream;
    private SinkStreamWriter assignedSinkStreamWriter;

    public SinkContext(SinkFactory factory, StreamDescriptor streamDescriptor) throws Exception {
   System.err.println("create sink context ");
      this.sink = factory.create(streamDescriptor);
      System.err.println("sink " + sink);
      System.err.println("stream descriptor " + streamDescriptor);
      this.assignedSinkStream = sink.getStream(streamDescriptor);
      System.err.println("stream " + assignedSinkStream);
      this.assignedSinkStreamWriter = this.assignedSinkStream.getWriter();

      System.err.println("writer " + assignedSinkStreamWriter);
    }

    public void prepareCommit() throws Exception {
      assignedSinkStreamWriter.prepareCommit();
    }

    public void completeCommit() throws Exception {
      assignedSinkStreamWriter.completeCommit();
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