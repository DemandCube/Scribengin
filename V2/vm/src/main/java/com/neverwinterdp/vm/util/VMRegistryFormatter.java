package com.neverwinterdp.vm.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;

public class VMRegistryFormatter extends NodeFormatter {
  private Node vmNode ;
  
  public VMRegistryFormatter(Node vmNode) {
    this.vmNode = vmNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      if(!vmNode.exists()) {
        return "VM node " + vmNode.getPath() + " is already deleted or moved to the history" ;
      }
      
      VMDescriptor vmDescriptor = vmNode.getDataAs(VMDescriptor.class);
      VMStatus vmStatus = null;
      boolean heartbeat = false;
      if(vmNode.hasChild("status")){
        vmStatus = vmNode.getChild("status").getDataAs(VMStatus.class);
      } else{
        return "VM node " + vmNode.getPath() + " has no status child" ;
      }
       
      if(vmNode.hasChild("status") && vmNode.getChild("status").hasChild("heartbeat")){
        heartbeat = vmNode.getChild("status").getChild("heartbeat").exists();
      }
      
      String roles="";
      for(String role: vmDescriptor.getVmConfig().getRoles()){
        roles = roles.concat(role+",");
      }
      
      String hbeat="";
      if(heartbeat){
        hbeat = "CONNECTED";
      } else{
        hbeat = "DISCONNECTED";
      }
      
      TabularFormater formatter = new TabularFormater("VMKey", "Value");
      formatter.addRow("Name",        vmDescriptor.getVmConfig().getName());
      formatter.addRow("Hostname",    vmDescriptor.getHostname());
      formatter.addRow("Description", vmDescriptor.getVmConfig().getDescription());
      formatter.addRow("Memory",      vmDescriptor.getMemory());
      formatter.addRow("CPU Cores",   vmDescriptor.getCpuCores());
      formatter.addRow("Stored Path", vmDescriptor.getStoredPath());
      formatter.addRow("Roles",       roles);
      if(vmStatus != null){
        formatter.addRow("Status",      vmStatus);
      } else{
        formatter.addRow("Status",      "STATUS UNAVAILABLE!");
      }
      formatter.addRow("Heartbeat",   hbeat);
      
      b.append(formatter.getFormatText());
      
      
    } catch (RegistryException e) {
      b.append(ExceptionUtil.getStackTrace(e));
    }
    return b.toString();
  }
}