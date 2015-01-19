package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.jetty.JettyServer;

public class CommandServer {
  public static class CommandServerCommandLine{
    @Parameter(names = { "-p", "--port" }, description = "Port to run on")
    public int port = 8181;
    @Parameter(names = {"-r", "--resourceBase"}, description = "Path to folder for base of server")
    public String resourceBase = null;
    @Parameter(names = {"-d", "--descriptor"}, description = "Path to web.xml you want to override")
    public String descriptor = null;
  }
  
  
  protected int port;
  protected JettyServer server;
  
  public CommandServer(int port){
    this.port = port;
    server = new JettyServer(this.port, CommandServlet.class);
  }
  
  public CommandServer(){
    this(8181);
  }
  
  public void startServer() throws Exception{
    server.start();
  }
  
  public void setHandler(WebAppContext context){
    server.setHandler(context);
  }
  
  public void join() throws Exception{
    server.join();
  }
  
  public void stop() throws Exception{
    server.stop();
  }
  
  public static void main(String args[]) throws Exception{
    CommandServerCommandLine  c = new CommandServerCommandLine();
    
    new JCommander(c, args);
    CommandServer cs = new CommandServer(c.port);
    if(c.descriptor != null && c.resourceBase != null){
      WebAppContext proxyApp = new WebAppContext();
      proxyApp.setResourceBase(c.resourceBase);
      proxyApp.setDescriptor(c.descriptor);
      cs.setHandler(proxyApp);
    }
    cs.startServer();
    cs.join();
  }
}
