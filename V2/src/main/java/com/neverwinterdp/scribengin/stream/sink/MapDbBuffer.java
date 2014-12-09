package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class MapDbBuffer implements DiskBuffer {

  private DB db;
  private ConcurrentNavigableMap<String, byte[]> map;

  public MapDbBuffer() {
    db = DBMaker.newFileDB(new File("tuplesFile")).closeOnJvmShutdown().make();
    map = db.getTreeMap("tuplesMap");
  }


  @Override
  public List<File> getAllFiles() {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public void add(Tuple tuple) {
    // map.put(path, t.getData());
    db.commit();
    
  }


  @Override
  public int getTuplesCount() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public int getTuplesSize() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public long getDuration() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public void clean() {
    // TODO Auto-generated method stub
    
  }

}
