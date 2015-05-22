package com.neverwinterdp.registry.zk;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.MultiDataGet;
import com.neverwinterdp.util.JSONSerializer;

public class ZookeeperMultiDataGet<T> implements MultiDataGet<T>, AsyncCallback.DataCallback {
  private RegistryImpl registry ;
  private Class<T>     type ;
  private LinkedHashMap<String, DataGet<T>> results = new LinkedHashMap<String, DataGet<T>>() ;
  private int processResultCount = 0;
  private int processErrorCount = 0 ;
  
  public ZookeeperMultiDataGet(RegistryImpl registry, Class<T> type) {
    this.registry = registry ;
    this.type = type ;
  }
  
  @Override
  public int  getProcessErrorGetCount() { return this.processErrorCount ; }
  
  @Override
  public int  getProcessResultCount() { return this.processResultCount; }
  
  
  @Override
  public void get(String path) {
    ZooKeeper zk = registry.getZkClient() ;
    String realPath = registry.realPath(path);
    zk.getData(realPath, false, this, null);
    DataGet<T> dataGet = new DataGet<T>(path, null);
    results.put(path, dataGet);
  }

  
  @Override
  public List<T>          getResults() {
    List<T> holder = new ArrayList<T>() ;
    for(DataGet<T> sel : results.values()) {
      holder.add(sel.getData()) ;
    }
    return holder ;
  }
  
  @Override
  public List<DataGet<T>> getDataGetResults() {
    return null ;
  }

  @Override
  synchronized public void waitForAllGet(long timeout) throws InterruptedException {
    long currentTime = System.currentTimeMillis() ;
    long stopTime = currentTime + timeout ;
    long waitTime = timeout ;
    while(waitTime > 0) {
      wait(waitTime) ;
      if(this.processResultCount == results.size()) return ;
      currentTime = System.currentTimeMillis();
      waitTime = stopTime - currentTime;
    }
  }
  
  @Override
  public void processResult(int rc, String realPath, Object ctx, byte[] data, Stat stat) {
    processResultCount++ ;
    String path = registry.path(realPath);
    DataGet<T> dataGet = results.get(path);
    if (rc == KeeperException.Code.OK.intValue()) {
      dataGet.setData(JSONSerializer.INSTANCE.fromBytes(data, type));
    } else {
      processErrorCount++ ;
      dataGet.setErrorCode(ErrorCode.Unknown);
    }
    synchronized(this) {
      notifyAll();
    }
  }
}
