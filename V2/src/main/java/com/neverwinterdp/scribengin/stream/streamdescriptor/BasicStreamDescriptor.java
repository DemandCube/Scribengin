package com.neverwinterdp.scribengin.stream.streamdescriptor;

public class BasicStreamDescriptor implements StreamDescriptor{
  String name;
  
  public BasicStreamDescriptor(String name){
    this.name = name;
  }
  
  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

}
