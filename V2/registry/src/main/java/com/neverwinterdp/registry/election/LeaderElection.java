package com.neverwinterdp.registry.election;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.util.JSONSerializer;

public class LeaderElection {
  private Registry  registry ;
  private String    electionPath ;
  private LeaderId  leaderId ;
  private LeaderElectionListener listener ;
  private boolean elected = false ;
  private Node node ;
  private byte[] info = {};
  
  public LeaderElection(Registry registry, String electionPath) {
    this.registry = registry;
    this.electionPath = electionPath ;
  }
  
  public <T> LeaderElection(Registry registry, String electionPath, byte[] info) {
    this(registry, electionPath);
    this.info = info;
  }
  
  public <T> LeaderElection(Registry registry, String electionPath, T info) {
    this(registry, electionPath);
    this.info = JSONSerializer.INSTANCE.toBytes(info);
  }
  
  public Registry getRegistry() { return this.registry ; }
  
  public String getElectionPath() { return this.electionPath ; }
  
  public void setListener(LeaderElectionListener listener) {
    this.listener = listener ;
  }
  
  public boolean isElected() { return this.elected ; }
  
  public LeaderId getLeaderId() { return this.leaderId ; }
  
  public Node getNode() { return this.node; }
  
  public void start() throws RegistryException {
    if(leaderId != null) {
      throw new RegistryException(ErrorCode.Unknown, "This leader election is already started") ;
    }
    String lockPath = electionPath + "/leader-" ;
    node = registry.create(lockPath , info, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    leaderId = new LeaderId(node.getPath()) ;
    LeaderWatcher watcher = new LeaderWatcher() ;
    watcher.watch();
  }
  
  public void stop() throws RegistryException {
    registry.delete(leaderId.getPath());
    elected = false ;
    leaderId = null ;
    node = null ;
  }
  
  private SortedSet<LeaderId> getSortedLockIds() throws RegistryException {
    List<String> names = registry.getChildren(electionPath) ;
    SortedSet<LeaderId> sortedLockIds = new TreeSet<LeaderId>();
    for (String nodeName : names) {
      if(nodeName.startsWith("leader-")) {
        sortedLockIds.add(new LeaderId(electionPath + "/" + nodeName));
      }
    }
    return sortedLockIds;
  }
  
  class LeaderWatcher extends NodeWatcher {
    @Override
    public void onEvent(NodeEvent event) {
      try {
        System.err.println("Election event: path = " + event.getType() + ", type" + event.getType());
        watch() ;
      } catch(RegistryException ex) {
        throw new RuntimeException(ex) ;
      }
    }
    
    public void watch() throws RegistryException {
      SortedSet<LeaderId> leaderIds = getSortedLockIds() ;
      LeaderId ownerId = leaderIds.first() ;
      if(ownerId.equals(leaderId)) {
        listener.onElected();
        elected = true ;
        return ;
      }
      SortedSet<LeaderId> lessThanMe = leaderIds.headSet(leaderId);
      LeaderId previousLock = lessThanMe.last();
      registry.watchExists(previousLock.getPath(), this);
    }
  }
}
