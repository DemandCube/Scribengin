package com.neverwinterdp.registry.lock;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class Lock {
  private Registry  registry ;
  private String    lockDir ;
  private String    name ;
  private LockId    lockId ;
  
  public Lock(Registry registry, String dir, String name) {
    this.registry = registry;
    this.lockDir = dir ;
    this.name = name ;
  }
  
  public Registry getRegistry() { return this.registry ; }
  
  public String getName() { return this.name ; }
  
  public String getLockDir() { return this.lockDir ; }
  
  public LockId lock(long timeout) throws RegistryException {
    if(lockId != null) {
      throw new RegistryException(ErrorCode.Unknown, "The lock is already created") ;
    }
    String lockPath = lockDir + "/" + name + "-" + registry.getSessionId() + "-" ;
    Node node = registry.create(lockPath , new byte[0], NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    lockId = new LockId(node.getPath()) ;
    SortedSet<LockId> currentLockIds = getSortedLockIds() ;
    LockId ownerId = currentLockIds.first() ;
    if(ownerId.equals(lockId)) {
      return lockId ;
    } else {
      LockWatcher watcher = new LockWatcher(timeout) ;
      watcher.watch(currentLockIds);
      watcher.waitForLock();
    }
    return lockId ;
  }
  
  public void unlock() throws RegistryException {
    if(lockId == null) return ;
    registry.delete(lockId.getPath());
    lockId = null ;
  }
  
  public void execute(LockOperation op, long timeout) throws RegistryException {
    lock(timeout) ;
    try {
      op.execute();
    } catch (Exception e) {
      throw new RegistryException(ErrorCode.Unknown, e);
    } finally {
      unlock();
    }
  }
  
  private SortedSet<LockId> getSortedLockIds() throws RegistryException {
    List<String> names = registry.getChildren(lockDir) ;
    SortedSet<LockId> sortedLockIds = new TreeSet<LockId>();
    for (String nodeName : names) {
      if(nodeName.startsWith(this.name)) {
        sortedLockIds.add(new LockId(lockDir + "/" + nodeName));
      }
    }
    return sortedLockIds;
  }
  
  class LockWatcher implements NodeWatcher {
    long startTime;
    long timeout ;
    
    public LockWatcher(long timeout) {
      this.startTime = System.currentTimeMillis();
      this.timeout = timeout ;
    }
    
    @Override
    public void process(NodeEvent event) {
      try {
        SortedSet<LockId> currentLockIds = getSortedLockIds() ;
        LockId ownerId = currentLockIds.first() ;
        if(ownerId.equals(lockId)) {
          synchronized(this) {
            notifyAll() ;
          }
          return ;
        } else {
          watch(currentLockIds);
        }
      } catch(RegistryException ex) {
        throw new RuntimeException(ex) ;
      }
    }
    
    public void watch(SortedSet<LockId> currentLockIds) throws RegistryException {
      SortedSet<LockId> lessThanMe = currentLockIds.headSet(lockId);
      LockId previousLock = lessThanMe.last();
      registry.watchModify(previousLock.getPath(), this);
    }
    
    public void waitForLock() throws RegistryException {
      synchronized(this) {
        try {
          wait(timeout - (System.currentTimeMillis() - startTime));
        } catch (InterruptedException e) {
          throw new RegistryException(ErrorCode.Timeout, e) ;
        }
      }
    }
  }
}
