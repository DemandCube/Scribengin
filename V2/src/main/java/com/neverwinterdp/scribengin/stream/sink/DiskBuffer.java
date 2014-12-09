package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.scribengin.tuple.Tuple;

public interface DiskBuffer {

  public void add(Tuple tuple);
  public List<File> getAllFiles();
  public int getTuplesCount();
  public int getTuplesSize();
  public long getDuration();
  public void clean();

}
