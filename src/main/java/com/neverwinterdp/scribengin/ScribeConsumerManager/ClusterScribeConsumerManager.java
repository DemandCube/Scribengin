package com.neverwinterdp.scribengin.ScribeConsumerManager;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.cluster.ScribeConsumerStatusCommand;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.ServerRegistration;
import com.neverwinterdp.server.cluster.ClusterMember;
import com.neverwinterdp.server.cluster.hazelcast.HazelcastClusterClient;
import com.neverwinterdp.server.command.ServiceCommand;
import com.neverwinterdp.server.command.ServiceCommandResult;
import com.neverwinterdp.server.service.ServiceRegistration;
import com.neverwinterdp.server.shell.Shell;

public class ClusterScribeConsumerManager extends AbstractScribeConsumerManager{
  private static final Logger LOG = Logger.getLogger(ClusterScribeConsumerManager.class.getName());
  List<ServerInfo> servers = new LinkedList<ServerInfo>();
  
  
  private class ServerInfo{
    public Server server;
    public ScribeConsumerConfig conf;
    
    public ServerInfo(Server s, ScribeConsumerConfig c){
      this.server = s;
      this.conf = c;
    }
  }
  
  public ClusterScribeConsumerManager(){
    
  }
  
  
  @Override
  public boolean startNewConsumers(ScribeConsumerConfig c, List<String> topics) {
    boolean retVal = true;
    for(String t: topics){
      c.topic = t;
      if(!this.startNewConsumer(c)){
        retVal = false;
      }
    }
    return retVal;
  }
  
  @Override
  public boolean startNewConsumer(ScribeConsumerConfig c) {
    Server scribeConsumer = Server.create("-Pserver.name=scribemaster", "-Pserver.roles=scribeconsumer");
    Shell shell = new Shell() ;
    shell.getShellContext().connect();
    String installScript ="module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribeconsumer:precommitpathprefix=" + c.PRE_COMMIT_PATH_PREFIX +
        " -Pscribeconsumer:commitpathprefix=" + c.COMMIT_PATH_PREFIX +
        " -Pscribeconsumer:topic="+ c.topic +
        " -Pscribeconsumer:partition=" + Integer.toString(c.partition) +
        " -Pscribeconsumer:brokerList=" + c.getBrokerListAsString() +
        " -Pscribeconsumer:commitCheckPointInterval="+ Long.toString(c.commitCheckPointInterval);
    if(c.hdfsPath != null){
      installScript += " -Pscribeconsumer:hdfsPath="+c.hdfsPath;
    }
    if(c.cleanStart){
      installScript += " -Pscribeconsumer:cleanStart=True";
    }
    installScript +=
        " --member-role scribeconsumer --autostart --module ScribeConsumer \n";
    shell.executeScript(installScript);
    
    servers.add(new ServerInfo(scribeConsumer, c));
    
    return true;
  }

  @Override
  public void monitorConsumers() {
    HazelcastClusterClient client = new HazelcastClusterClient() ;
    
    Iterator<ServerInfo> it = servers.iterator();
    while (it.hasNext()) {
      ServerInfo si = it.next();
      ClusterMember member = si.server.getClusterService().getMember();
      ServerRegistration serverRegistration = client.getServerRegistration(member);
      ServiceRegistration statusService = serverRegistration.getServices().get(0) ;
      ServiceCommand<Thread.State> serverState = new ScribeConsumerStatusCommand().setLogEnable(true) ;
      serverState.setTargetService(statusService);
      ServiceCommandResult<Thread.State> serverThreadState = client.execute(serverState, member) ;
      
      if(serverThreadState.hasError()) {
        LOG.error("Error getting thread state: "+serverThreadState.getError());
        continue;
      }
      Thread.State state = serverThreadState.getResult();
      //Basically ScribeConsumer should never die, so restart it if something happens
      if(!(state == Thread.State.NEW || state == Thread.State.RUNNABLE) || state == null){
        LOG.error("Server in bad state.  Thread state: "+state.toString()+" Topic: "+si.conf.topic);
        if(startNewConsumer(si.conf)){
          it.remove();
        }
      }
      else{
        System.err.println("SERVER FUCKING STATE: "+state.toString());
      }
    }
  }

  @Override
  public boolean shutdownConsumers() {
    boolean retVal = true;
    Iterator<ServerInfo> iterator = servers.iterator();
    while(iterator.hasNext()){
      ServerInfo si = iterator.next();
      try{
        si.server.destroy();
        iterator.remove();
      } catch(Exception e){
        e.printStackTrace();
        retVal = false;
      }
    }
    return retVal;
  }


  @Override
  public int getNumConsumers() {
    return servers.size();
  }
  
}
