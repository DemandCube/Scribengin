package com.neverwinterdp.registry.lock;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;

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
  
  public <T> T execute(BatchOperations<T> op, int retry, long timeoutThreshold) throws RegistryException {
    for(int i = 0;i < retry; i++) {
      try {
        lock(timeoutThreshold * (i + 1)) ;
        T result = op.execute(registry);
        unlock();
        return result;
      } catch (RegistryException e) {
        if(e.getErrorCode() != ErrorCode.Timeout) {
          throw e;
        }
      } catch (Exception e) {
        throw new RegistryException(ErrorCode.Unknown, e);
      }
    }
    throw new RegistryException(ErrorCode.Unknown, "Fail after " + retry + "tries");
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
  
  class LockWatcher extends NodeWatcher {
    long    startTime;
    long    timeout;
    boolean obtainedLock = false;
    
    public LockWatcher(long timeout) {
      this.startTime = System.currentTimeMillis();
      this.timeout = timeout ;
    }
    
    @Override
    public void onEvent(NodeEvent event) {
      try {
        if(event.getType() != NodeEvent.Type.DELETE) {
          return ;
        }
        SortedSet<LockId> currentLockIds = getSortedLockIds() ;
        LockId ownerId = currentLockIds.first() ;
        if(ownerId.equals(lockId)) {
          synchronized(this) {
            obtainedLock = true ;
            notifyAll() ;
          }
          return ;
        } else {
          watch(currentLockIds);
        }
      } catch(RegistryException ex) {
        throw new RuntimeException("Error lock " + lockDir, ex) ;
      }
    }
    
    public void watch(SortedSet<LockId> currentLockIds) throws RegistryException {
      SortedSet<LockId> lessThanMe = currentLockIds.headSet(lockId);
      LockId previousLock = lessThanMe.last();
      registry.watchExists(previousLock.getPath(), this);
    }
    
    public void waitForLock() throws RegistryException {
      synchronized(this) {
        try {
          wait(timeout - (System.currentTimeMillis() - startTime));
        } catch (InterruptedException e) {
          throw new RegistryException(ErrorCode.Timeout, e) ;
        }
        if(!obtainedLock) {
          //check again
          SortedSet<LockId> currentLockIds = getSortedLockIds() ;
          LockId ownerId = currentLockIds.first() ;
          if(ownerId.equals(lockId)) {
            obtainedLock = true;
          } else {
            String lockIdPath = lockId.getPath();
            registry.delete(lockIdPath);
            lockId = null;
            throw new RegistryException(ErrorCode.Timeout, "Cannot obtain a lock at " + lockIdPath + " after " + timeout + "ms") ;
          }
        }
      }
    }
  }
}
