package com.neverwinterdp.testSink;

import java.util.ArrayList;

import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;

public class TestSink implements Sink {

  ArrayList<TestSinkStream> streams;
  SinkDescriptor sinkDesc;
  
  
  public TestSink(SinkDescriptor desc){
    this.sinkDesc = desc;
    this.streams = new ArrayList<TestSinkStream>();
    
  }
  
  @Override
  public SinkDescriptor getDescriptor() {
    return this.sinkDesc;
  }

  @Override
  public SinkStream getStream(SinkStreamDescriptor descriptor) throws Exception {
    if(streams.size() > 0 ){
      SinkStream stream = streams.get(descriptor.getId());
      if(stream != null) return stream ;
    }
    TestSinkStream newStream= new TestSinkStream(descriptor) ;
    streams.add( newStream) ;
    return newStream;
  }

  @Override
  public SinkStream[] getStreams() {
    SourceStream[] array = new SourceStream[streams.size()];
    return (SinkStream[]) streams.toArray(array);
  }

  @Override
  public void delete(SinkStream stream) throws Exception {
    this.streams.remove(stream);
  }

  @Override
  public SinkStream newStream() throws Exception {
    SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor(this.sinkDesc);
    return new TestSinkStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
  }

}
