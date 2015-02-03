package com.neverwinterdp.scribengin.kafka.sink;

import java.util.LinkedHashMap;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.scribengin.kafka.KafkaClient;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class KafkaSink implements Sink {
  private SinkDescriptor descriptor;
  private KafkaClient kafkaClient ;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, KafkaSinkStream> streams = new LinkedHashMap<Integer, KafkaSinkStream>() ;
  
  public KafkaSink(SinkDescriptor descriptor) throws Exception {
    init(descriptor) ;
  }
  
  public KafkaSink(String name, String zkConnect, String topic) throws Exception {
    SinkDescriptor descriptor = new SinkDescriptor("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    init(descriptor);
  }
  
  private void init(SinkDescriptor descriptor) throws Exception {
    kafkaClient = new KafkaClient(descriptor.attribute("name"), descriptor.attribute("zk.connect")) ;
    kafkaClient.connect();
    descriptor.attribute("broker.list", kafkaClient.getKafkaBrokerList());
    this.descriptor  = descriptor ;
  }
  
  @Override
  public SinkDescriptor getDescriptor() { return descriptor; }

  @Override
  public SinkStream getStream(SinkStreamDescriptor descriptor) throws Exception {
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
    SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor(this.descriptor);
    streamDescriptor.setId(idTracker++);
    return new KafkaSinkStream(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(KafkaSinkStream sel : streams.values()) {
    }
    kafkaClient.close();
  }
}
