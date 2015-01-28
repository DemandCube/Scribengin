package com.neverwinterdp.command.server;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.client.shell.Shell;

@SuppressWarnings("serial")
public class CommandServlet extends HttpServlet {
  public static String noCommandMessage = "No Command Sent";
  public static String badCommandMessage = "Bad Command: ";
  private Shell vmShell; 
  private CommandConsole shellConsole;
  RegistryConfig regConf;
  Registry reg;

  @Override
  public void init() throws ServletException {
    this.regConf = new RegistryConfig();
    ServletConfig conf = this.getServletConfig();
    
    //Get config from web.xml
    this.regConf.setConnect(conf.getInitParameter("host"));
    this.regConf.setDbDomain("/NeverwinterDP");
    
    shellConsole = new CommandConsole();
    
    try {
      this.reg = new RegistryImpl(this.regConf);
      vmShell = new ScribenginShell(this.reg.connect(), shellConsole);
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
  }
  
  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response ) throws ServletException,IOException{
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().print("Hello from commandServlet");
  }
  
  
  @Override 
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException{
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    
    String command = request.getParameter("command");
    
    /*
    @SuppressWarnings("unchecked")
    Enumeration<String> x = request.getParameterNames();
    while(x.hasMoreElements()){
      System.err.println(x.nextElement());
    }
    
    @SuppressWarnings("unchecked")
    x = request.getHeaderNames();
    while(x.hasMoreElements()){
      System.err.println(x.nextElement());
    }
    
    @SuppressWarnings("unchecked")
    x = request.getAttributeNames();
    while(x.hasMoreElements()){
      System.err.println(x.nextElement());
    }
    System.err.println("");
    */
    
    if(command == null){
      response.getWriter().print(noCommandMessage);
    }
    else{
      switch(command){
        case "vm list":
          response.getWriter().print(executeShell("vm list"));
          break;
        case "registry dump":
          String path = request.getParameter("path");
          if(path == null){
            response.getWriter().print(executeShell("registry dump"));
          }
          else{
            response.getWriter().print(executeShell("registry dump --path "+path));
          }
          break;
        case "scribengin master":
          response.getWriter().print(executeShell("scribengin master"));
          break;
        case "dataflow":
          ScribenginClient sc = new ScribenginClient(this.reg);
          DataflowSubmitter submitter = new DataflowSubmitter(sc);
          try {
            submitter.submit(request);
          } catch (Exception e) {
            response.getWriter().print("DATAFLOW ERROR: "+e.getMessage());
          }
          response.getWriter().print("DATAFLOW SUBMITTED SUCCESSFULLY");
          break;
        default:
          response.getWriter().print(badCommandMessage+command);
      }
    }
  }
  
  protected String executeShell(String command){
    try {
      vmShell.execute(command);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return shellConsole.getLastCommandsOutput();
  }
  
  
}
