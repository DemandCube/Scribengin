package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.jetty.JettyServer;

public class CommandProxyServer extends CommandServer{
  
  public CommandProxyServer(int port){
    this.port = port;
    server = new JettyServer(port, CommandProxyServlet.class);
  }
  
  public CommandProxyServer(){
    this(8383);
  }
  
  @Override
  public void startServer() throws Exception{
    server.start();
  }
  

  public static void main(String args[]) throws Exception{
    CommandServerCommandLine  c = new CommandServerCommandLine();
    
    new JCommander(c, args);
    CommandProxyServer cps = new CommandProxyServer(c.port);
    if(c.descriptor != null && c.resourceBase != null){
      WebAppContext proxyApp = new WebAppContext();
      proxyApp.setResourceBase(c.resourceBase);
      proxyApp.setDescriptor(c.descriptor);
      cps.setHandler(proxyApp);
    }
    cps.startServer();
    cps.join();
  }
}
