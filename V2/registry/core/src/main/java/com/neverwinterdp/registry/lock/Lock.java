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
  private Registry    registry ;
  private String      lockDir ;
  private String      name ;
  private LockId      lockId ;
  
  private String      description;
  private LockWatcher currentLockWatcher;
  
  public Lock(Registry registry, String dir, String name) {
    this(registry, dir, name, "") ;
  }
  
  public Lock(Registry registry, String dir, String name, String desc) {
    this.registry = registry;
    this.lockDir = dir ;
    this.name = name ;
    this.description = desc; 
  }
  
  public Registry getRegistry() { return this.registry ; }
  
  public String getName() { return this.name ; }
  
  public String getLockDir() { return this.lockDir ; }
  
  public String getDescription() { return this.description; }
  
  public LockId lock(long timeout) throws RegistryException {
    if(lockId != null) {
      throw new RegistryException(ErrorCode.Unknown, "The lock is already created") ;
    }
    String lockPath = lockDir + "/" + name + "-" + registry.getSessionId() + "-" ;
    Node node = registry.create(lockPath , description.getBytes(), NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    lockId = new LockId(node.getPath()) ;
    SortedSet<LockId> currentLockIds = getSortedLockIds() ;
    currentLockWatcher = new LockWatcher(timeout) ;
    currentLockWatcher.tryWatch(currentLockIds);
    currentLockWatcher.waitForLock();
    return lockId ;
  }
  
  public void unlock() throws RegistryException {
    if(lockId == null) return ;
    registry.delete(lockId.getPath());
    lockId = null ;
    currentLockWatcher = null ;
  }
  
  public <T> T execute(BatchOperations<T> op, int retry, long timeoutThreshold) throws RegistryException {
    RegistryException lastError = null ;;
    for(int i = 0;i < retry; i++) {
      try {
        lock(timeoutThreshold * (i + 1)) ;
        T result = op.execute(registry);
        return result;
      } catch (RegistryException e) {
        if(e.getErrorCode() == ErrorCode.Connection || e.getErrorCode() == ErrorCode.Timeout) {
          lastError = e;
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new RegistryException(ErrorCode.Unknown, e);
      } finally {
        unlock();
      }
    }
    throw lastError;
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
      if(event.getType() != NodeEvent.Type.DELETE) return ;
      try {
        SortedSet<LockId> currentLockIds = getSortedLockIds() ;
        tryWatch(currentLockIds);
      } catch(Throwable ex) {
        throw new RuntimeException("Error lock " + event.getPath() + ", event = " + event.getType() + ", lock id = " + lockId, ex) ;
      }
    }
    
    synchronized public void tryWatch(SortedSet<LockId> currentLockIds) throws RegistryException {
      while(true) {
        try {
          LockId ownerId = currentLockIds.first() ;
          if(ownerId.equals(lockId)) {
            obtainedLock = true ;
            notifyAll() ;
            return ;
          } else {
            SortedSet<LockId> lessThanMe = currentLockIds.headSet(lockId);
            LockId previousLock = lessThanMe.last();
            registry.watchModify(previousLock.getPath(), this);
            return;
          }
        } catch(RegistryException ex) {
          if(ex.getErrorCode() == ErrorCode.NoNode) {
            currentLockIds = getSortedLockIds() ;
            continue;
          }
          throw ex ;
        }
      }
    }
    
    synchronized public void waitForLock() throws RegistryException {
      if(obtainedLock) return;
      try {
        wait(timeout - (System.currentTimeMillis() - startTime));
      } catch (InterruptedException e) {
        setComplete() ;
        unlock();
        throw new RegistryException(ErrorCode.Timeout, e) ;
      }
      if(!obtainedLock) {
        String lockIdPath = lockId.getPath();
        setComplete() ;
        unlock();
        throw new RegistryException(ErrorCode.Timeout, "Cannot obtain a lock at " + lockIdPath + " after " + timeout + "ms") ;
      }
    }
  }
}