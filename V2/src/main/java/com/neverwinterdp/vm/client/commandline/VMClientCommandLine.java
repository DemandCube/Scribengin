package com.neverwinterdp.vm.client.commandline;

import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.client.VMClient;


public class VMClientCommandLine extends VMClient{
  public static class CommandLine{
    @Parameter(names = { "-r", "--registry" }, description = "Hostname of registry to connect to")
    public String registryHostname = null;
  
    @Parameter(names = { "-g", "--getRegistry" }, description = "Print out raw state of registry")
    public boolean getRegistry = false;
    
    @Parameter(names = { "-d", "--dbDomain" }, description = "Registry Root.  Default is /NeverwinterDP")
    public String dbDomain = "/NeverwinterDP";
  }
  
  
  public VMClientCommandLine(Registry r) {
    super(r);
    try {
      this.getRegistry().connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
  }
  
  public String getDump(String parent, String node, String indentation)  {
    try {
      return dump(parent, node, indentation);
    } catch (IOException | RegistryException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  public String getDump(String parent, String node) {
    return getDump(parent, node, " ");
  }
  
  public String getDump() {
    return getDump("/", "", " ");
  }
  
  private String dump(String parent, String node, String indentation) throws IOException, RegistryException {
    String path = parent + "/" + node;
    //System.err.println("PARENT: "+parent);
    //System.err.println("NODE: "+node);
    if("/".equals(parent)) {
      path = "/" + node;
    }
    //System.err.println("PATH: "+ path);
    
    byte[] data = this.getRegistry().getData(path);
    
    String stringData = "";
    if(data != null && data.length > 0) {
      stringData = " - " + new String(data);
      stringData = stringData.replace("\r\n", " ");
      stringData = stringData.replace("\n", " ");
      //if(stringData.length() > 80) {
      //  stringData = stringData.substring(0, 80);
      //}
    }
    
    String res = indentation + node + stringData+"\n";
    List<String> children = this.getRegistry().getChildren(path);
    for(String child : children) {
      res += dump(path, child, indentation+" ");
    }
    
    return res;
  }
  
  public String getRegistryValue(String path){
    byte[] data;
    try {
      data = this.getRegistry().getData(path);
    } catch (RegistryException e) {
      e.printStackTrace();
      return null;
    }
    return new String(data);
  }
  
  private String splitPathAndGetDump(String fullPath){
    int i = fullPath.lastIndexOf("/");
    return getDump(fullPath.substring(0, i), fullPath.substring(i+1));
  }
  
  public String getVMHistory(){
    return splitPathAndGetDump(VMService.HISTORY_PATH);
  }
  
  public String getVMAllocated(){
    return splitPathAndGetDump(VMService.ALLOCATED_PATH);
  }
  
  public String getVMLeader(){
    return splitPathAndGetDump(VMService.LEADER_PATH);
  }
  
  public VMDescriptor parseStringToVMDescriptor(String regInput){
    ObjectMapper mapper = new ObjectMapper();
    VMDescriptor vmDesc = null;
    try {
      vmDesc = mapper.readValue(regInput, VMDescriptor.class);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return vmDesc;
    //To get it back as a String - 
    //System.out.println(mapper.writeValueAsString(vmDesc));
  }
  
  public String getJsonString(VMDescriptor vmDesc){
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(vmDesc);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  
  public static void main(String args[]) throws RegistryException, IOException{
    CommandLine  c = new CommandLine();
    
    new JCommander(c, args);
    
    RegistryConfig regConf = new RegistryConfig();
    regConf.setConnect(c.registryHostname);
    regConf.setDbDomain(c.dbDomain);
    
    
    VMClientCommandLine client = new VMClientCommandLine(new RegistryImpl(regConf));
    
    if(c.getRegistry){
      System.out.println(client.getDump());
    }
  }
}
