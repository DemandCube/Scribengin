package com.neverwinterdp.scribengin.stream.streamdescriptor;

public class FileSinkStreamDescriptor implements StreamDescriptor{
  String dir;
  String filename;
  String name;

  public FileSinkStreamDescriptor(String name, String directory, String filename){
    this.name = name;
    this.dir = directory;
    this.filename = filename;
  }

  public String getDir() {
    return dir;
  }
  
  
  public String getFilename() {
    return filename;
  }

  @Override
  public String getName() {
    return this.name;
  }

  public void setDir(String dir) {
    this.dir = dir;
  }
  
  public void setFilename(String filename) {
    this.filename = filename;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

}
