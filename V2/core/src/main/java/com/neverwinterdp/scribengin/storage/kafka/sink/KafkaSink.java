package com.neverwinterdp.scribengin.storage.kafka.sink;

import java.util.LinkedHashMap;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;

public class KafkaSink implements Sink {
  private StorageDescriptor descriptor;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, KafkaSinkStream> streams = new LinkedHashMap<Integer, KafkaSinkStream>() ;
  
  public KafkaSink(StorageDescriptor descriptor) throws Exception {
    init(descriptor) ;
  }
  
  public KafkaSink(String name, String zkConnect, String topic) throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor("kafka");
    descriptor.attribute("dataflowName", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    init(descriptor);
  }
  
  private void init(StorageDescriptor descriptor) throws Exception {
    KafkaTool kafkaTool = new KafkaTool(descriptor.attribute("dataflowName"), descriptor.attribute("zk.connect")) ;
    kafkaTool.connect();
    descriptor.attribute("broker.list", kafkaTool.getKafkaBrokerList());
    this.descriptor  = descriptor ;
    kafkaTool.close();
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return descriptor; }

  @Override
  public SinkStream getStream(StreamDescriptor descriptor) throws Exception {
    SinkStream stream = streams.get(descriptor.getId());
    if(stream != null) return stream ;
    KafkaSinkStream newStream= new KafkaSinkStream(descriptor) ;
    streams.put(descriptor.getId(), newStream) ;
    return newStream;
  }

  @Override
  public SinkStream[] getStreams() {
    SinkStream[] array = new SinkStream[streams.size()];
    return streams.values().toArray(array);
  }

  @Override
  public void delete(SinkStream stream) throws Exception {
    SinkStream found = streams.get(stream.getDescriptor().getId());
    if(found != null) {
      found.delete();
      streams.remove(stream.getDescriptor().getId());
    } else {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getId());
    }
  }

  @Override
  public SinkStream newStream() throws Exception {
    StreamDescriptor streamDescriptor = new StreamDescriptor(this.descriptor);
    streamDescriptor.setId(idTracker++);
    return new KafkaSinkStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(KafkaSinkStream sel : streams.values()) {
    }
  }
}
