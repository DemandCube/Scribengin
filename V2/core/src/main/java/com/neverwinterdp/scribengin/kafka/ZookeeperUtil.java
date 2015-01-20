package com.neverwinterdp.scribengin.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.util.JSONSerializer;

public class ZookeeperUtil {
  static public void dump(ZooKeeper zkClient, String path) throws KeeperException, InterruptedException {
    System.out.println(path) ;
    List<String> children = zkClient.getChildren(path, false);
    for(int i = 0; i < children.size(); i++) {
      dump(zkClient, path, children.get(i), "  ");
    }
  }
  
  static  void dump(ZooKeeper zkClient, String parentPath, String node, String indentation) throws KeeperException, InterruptedException {
    String path = parentPath + "/" + node;
    byte[] bytes = zkClient.getData(path, false, new Stat());
    if(bytes != null) {
      String data = new String(bytes);
      if(data.length() > 120 ) data = data.substring(0, 120);
      System.out.println(indentation + node + " - " + data) ;
    } else {
      System.out.println(indentation + node) ;
    }
    List<String> children = zkClient.getChildren(path, false);
    String childIndentation = indentation + "  " ;
    for(int i = 0; i < children.size(); i++) {
      dump(zkClient, path, children.get(i), childIndentation);
    }
  }
  
  static public <T> T getDataAs(ZooKeeper zkClient, String path, Class<T> type) throws KeeperException, InterruptedException {
    byte[] data = zkClient.getData(path, false, new Stat());
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }
  
  static public <T> List<T> getChildrenDataAs(ZooKeeper zkClient, String path, Class<T> type) throws KeeperException, InterruptedException {
    List<T> holder = new ArrayList<T>();
    List<String> children = zkClient.getChildren(path, false);
    for(int i = 0; i < children.size(); i++) {
      byte[] data = zkClient.getData(path + "/" + children.get(i), false, new Stat());
      T obj = JSONSerializer.INSTANCE.fromBytes(data, type);
      holder.add(obj);
    }
    return holder;
  }
}