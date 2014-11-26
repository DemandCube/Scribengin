package com.neverwinterdp.scribengin.dataflow;

import java.util.Arrays;
import java.util.LinkedList;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.streamcoordinator.StreamCoordinator;

public class DataflowImpl implements Dataflow{
  private String name;
  private StreamCoordinator streamCoordinator;
  private LinkedList<Scribe> scribes;
  
  public DataflowImpl(String name, StreamCoordinator s){
    this.name = name;
    scribes = new LinkedList<Scribe>();
    streamCoordinator = s;
  }

  public void addStream(Scribe s){
    scribes.add(s);
  }
  
  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void pause() {
    for(Scribe s: scribes){
      s.stop();
    }
  }

  @Override
  public void stop() {
    for(Scribe s: scribes){
      s.stop();
    }
  }

  @Override
  public void start() {
    for(Scribe s: scribes){
      s.start();
    }
  }

  @Override
  public void initScribes() {
    scribes.addAll(Arrays.asList(streamCoordinator.allocateStreams()));
    for(Scribe s: scribes){
      s.init();
    }
  }

  @Override
  public Scribe[] getScribes() {
    return Arrays.copyOf( scribes.toArray(),  scribes.toArray().length, Scribe[].class);
  }

  @Override
  public void setStreamCoordinator(StreamCoordinator s) {
    streamCoordinator = s;
  }


}
