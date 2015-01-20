package com.neverwinterdp.registry.queue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;

/**
 * This queue implement the java.util.Queue api with the take method
 * @author Tuan
 */
public class DistributedQueue {
  private final String path;
  private Registry     registry;

  public DistributedQueue(Registry registry, String path) throws RegistryException {
    this.path = path;
    this.registry = registry;
    registry.createIfNotExist(path) ;
  }

  public String getPath() { return this.path ; }
  
  public Registry getRegistry() { return this.registry; }

  /**
   * Inserts the specified element into this queue if it is possible to do
   * so immediately without violating capacity restrictions.
   * When using a capacity-restricted queue, this method is generally
   * preferable to {@link #add}, which can fail to insert an element only
   * by throwing an exception.
   * @throws RegistryException 
   */
  public void offer(byte[] data) throws RegistryException {
    registry.create(path + "/", data, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }

  /**
   * Retrieves and removes the head of this queue.  This method differs
   * from {@link #poll poll} only in that it throws an exception if this
   * queue is empty.
   */
  public byte[] remove() throws RegistryException, Exception  {
    byte[] data = poll();
    if(data == null) throw new Exception("The queue is empty") ;
    return data;
  }

  /**
   * Retrieves and removes the head of this queue, or returns <tt>null</tt> if this queue is empty.
   */
  public byte[] poll() throws RegistryException {
    List<String> orderedChildren = orderedChildren();
    if(orderedChildren.size() == 0) return null ;
    String headChild = orderedChildren.get(0) ;
    String headChildPath = path + "/" + headChild ;
    byte[] data = registry.getData(headChildPath) ;
    registry.delete(headChildPath);
    return data;
  }

  /**
   * Retrieves, but does not remove, the head of this queue.  This method differs from {@link #peek peek} 
   * only in that it throws an exception if this queue is empty.
   */
  public byte[] element() throws RegistryException {
    byte[] data = peek() ;
    return data;
  }

  /**
   * Retrieves, but does not remove, the head of this queue, or returns <tt>null</tt> if this queue is empty.
   */
  public byte[] peek() throws RegistryException {
    List<String> orderedChildren = orderedChildren();
    if(orderedChildren.size() == 0) return null ;
    String headChild = orderedChildren.get(0) ;
    String headChildPath = path + "/" + headChild ;
    byte[] data = registry.getData(headChildPath) ;
    return data;
  }
  
  /**
   * This method suppose to wait if the queue is empty and return when the queue entry is available
   * @return
   * @throws RegistryException
   * @throws InterruptedException
   */
  public byte[] take() throws RegistryException, InterruptedException {
    while(true){
      List<String> orderedChildren = orderedChildren();
      if(orderedChildren.size() == 0) {
        LatchChildWatcher childWatcher = new LatchChildWatcher();
        registry.watchChildren(path, childWatcher);
        childWatcher.await();
        continue;
      }
      String headNode = orderedChildren.get(0);
      String headNodePath = path +"/"+headNode;
      byte[] data = registry.getData(headNodePath);
      registry.delete(headNodePath);
      return data;
    }
  }
  
  public <T> T takeAs(Class<T> type) throws RegistryException, InterruptedException {
    byte[] data = take() ;
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }
  
  /**
   * Returns a List of the children, ordered by id.
   */
  private List<String> orderedChildren() throws RegistryException  {
    List<String> orderedChildren = registry.getChildren(path);
    Collections.sort(orderedChildren);
    return orderedChildren;
  }
  
  private class LatchChildWatcher extends NodeWatcher {

    CountDownLatch latch;

    public LatchChildWatcher(){
      latch = new CountDownLatch(1);
    }

    @Override
    public void process(NodeEvent event) {
      latch.countDown();
    }
    
    public void await() throws InterruptedException {
      latch.await();
    }
  }
}