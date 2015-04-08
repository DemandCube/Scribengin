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
    //TODO: use TabularFormater
    StringBuilder b = new StringBuilder() ;
    try {
      VMDescriptor vmDescriptor = vmNode.getDataAs(VMDescriptor.class);
      VMStatus vmStatus = vmNode.getChild("status").getDataAs(VMStatus.class);
      boolean heartbeat = vmNode.getChild("status").getChild("heartbeat").exists();
      
      String roles="";
      for(String role: vmDescriptor.getVmConfig().getRoles()){
        roles.concat(role+",");
      }
      
      String hbeat="";
      if(heartbeat){
        hbeat = "CONNECTED";
      } else{
        hbeat = "DISCONNECTED";
      }
      
      TabularFormater formatter = new TabularFormater("Name", "Hostname", "Description", 
          "Memory", "CPU Cores", "Stored Path", "Roles", "Status", "Heartbeat");
      
      formatter.addRow(vmDescriptor.getVmConfig().getName(),
          vmDescriptor.getHostname(),
          vmDescriptor.getVmConfig().getDescription(),
          vmDescriptor.getMemory(),
          vmDescriptor.getCpuCores(),
          vmDescriptor.getStoredPath(),
          roles,
          vmStatus,
          hbeat);
      
      b.append(formatter.getFormatText());
      
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(ExceptionUtil.getStackTrace(e));
    }
    return b.toString();
  }
}