package com.neverwinterdp.scribengin.kafka.sink;

import java.util.LinkedHashMap;

import org.apache.zookeeper.KeeperException;

import com.neverwinterdp.scribengin.kafka.KafkaClient;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class SinkImpl implements Sink {
  private SinkDescriptor descriptor;
  private KafkaClient kafkaClient ;
  
  private int idTracker = 0;
  private LinkedHashMap<Integer, SinkStreamImpl> streams = new LinkedHashMap<Integer, SinkStreamImpl>() ;
  
  public SinkImpl(SinkDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  public SinkImpl(String name, String zkConnect, String topic) throws Exception {
    kafkaClient = new KafkaClient(name, zkConnect) ;
    kafkaClient.connect();
    descriptor = new SinkDescriptor("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    descriptor.attribute("broker.list", kafkaClient.getKafkaBrokerList());
  }
  
  @Override
  public SinkDescriptor getDescriptor() { return descriptor; }

  @Override
  public SinkStream getStream(SinkStreamDescriptor descriptor) throws Exception {
    return streams.get(descriptor.getId());
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
    return new SinkStreamImpl(streamDescriptor);
  }

  @Override
  public void close() throws Exception {
    for(SinkStreamImpl sel : streams.values()) {
    }
    kafkaClient.close();
  }
}
