package com.neverwinterdp.vm.util;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;

public class VMRegistryFormatter extends NodeFormatter {
  private Node vmNode ;
  
  public VMRegistryFormatter(Node vmNode) {
    this.vmNode = vmNode;
  }
  
  public Map<String,String> getFormattedMap(VMDescriptor vmDescriptor){
    String roles="";
    for(String role: vmDescriptor.getVmConfig().getRoles()){
      roles = roles.concat(role+",");
    }
    
    Map<String,String> result = new HashMap<String, String>();
    result.put("Name", vmDescriptor.getVmConfig().getName());
    result.put("Hostname", vmDescriptor.getHostname());
    result.put("Description", vmDescriptor.getVmConfig().getDescription());
    result.put("Memory", Integer.toString(vmDescriptor.getMemory()));
    result.put("CPU Cores", Integer.toString(vmDescriptor.getCpuCores()));
    result.put("Stored Path", vmDescriptor.getRegistryPath());
    result.put("Roles", roles);
    
    return result;
  }
  
  public Map<String,String> getFormattedMap(VMDescriptor vmDescriptor, VMStatus vmStatus, boolean heartbeat){
    Map<String,String> result = getFormattedMap(vmDescriptor);
    String hbeat="";
    if(heartbeat){
      hbeat = "CONNECTED";
    } else{
      hbeat = "DISCONNECTED";
    }
    result.put("Status", vmStatus.name());
    result.put("Heartbeat", hbeat);
    
    return result;
  }
  
  @Override
  public String getFormattedText() {
    
    VMDescriptor vmDescriptor = null; 
    VMStatus vmStatus = null;
    boolean heartbeat = false;
    try{
      if(!vmNode.exists()) {
        return "VM node " + vmNode.getPath() + " is already deleted or moved to the history" ; 
      }
      
      vmDescriptor = vmNode.getDataAs(VMDescriptor.class);
      if(vmNode.hasChild("status")){
        vmStatus = vmNode.getChild("status").getDataAs(VMStatus.class);
      } else{
        return "VM node " + vmNode.getPath() + " has no status!" ;
      }
       
      if(vmNode.hasChild("status") && vmNode.getChild("status").hasChild("heartbeat")){
        heartbeat = vmNode.getChild("status").getChild("heartbeat").exists();
      }
    } catch(Exception e){
      return "VM node " + vmNode.getPath() + " is already deleted or moved to the history" ; 
    }
    
    TabularFormater formatter = new TabularFormater("VMKey", "Value");
    Map<String,String> data = getFormattedMap(vmDescriptor, vmStatus, heartbeat);
    for (Map.Entry<String, String> entry : data.entrySet()) {
      formatter.addRow(entry.getKey(), entry.getValue());
    }
    
    
    return formatter.getFormatText();
  }
}