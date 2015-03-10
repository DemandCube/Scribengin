package com.neverwinterdp.command.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;

@SuppressWarnings("serial")
public class CommandServlet extends HttpServlet {
  public static String noCommandMessage = "No Command Sent";
  //public static String badCommandMessage = "Bad Command: ";
  private ScribenginShell scribenginShell; 
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
      this.scribenginShell = new ScribenginShell(this.reg.connect(), shellConsole);
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
        case "dataflow":
          ScribenginClient sc = new ScribenginClient(this.reg);
          try {
            sc.submit(DescriptorBuilder.parseDataflowInput(parseRequestIntoMap(request)));
          } catch (Exception e) {
            response.getWriter().print("DATAFLOW ERROR: "+e.getMessage());
          }
          String dataflowName = request.getParameter("dataflow-Name");
          if(dataflowName == null){
            dataflowName = DescriptorBuilderDefaults._dataflowName;
          }
          response.getWriter().print("DATAFLOW "+ dataflowName +" SUBMITTED SUCCESSFULLY");
          break;
        default:
          response.getWriter().print(executeShell(command));
      }
    }
  }
  
  protected String executeShell(String command){
    try {
      scribenginShell.execute(command);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return shellConsole.getLastCommandsOutput();
  }
  
  /**
   * To make the DescriptorBuilder more reusable, going to parse out all the entries from
   * the request parameter map.  There should only ever be 1 item per entry, so parse out the
   * need to have multiple values per key
   * @param request
   * @return
   */
  protected Map<String, String> parseRequestIntoMap(HttpServletRequest request){
    Map<String,String> result = new HashMap<String,String>();
    
    for (Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
      result.put(entry.getKey(), entry.getValue()[0]);
    }
    
    return result;
  }
  
}
