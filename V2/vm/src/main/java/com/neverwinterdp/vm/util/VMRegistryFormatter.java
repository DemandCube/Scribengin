package com.neverwinterdp.vm.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.util.ExceptionUtil;
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
      
      b.append("------------------------------------------------------------------------\n");
      b.append("Name         : ");
      b.append(vmDescriptor.getVmConfig().getName());
      b.append("\n");
      
      b.append("Hostname     : ");
      b.append(vmDescriptor.getHostname());
      b.append("\n");
      
      b.append("Description  : ");
      b.append(vmDescriptor.getVmConfig().getDescription());
      b.append("\n");
      
      b.append("Memory       : ");
      b.append(vmDescriptor.getMemory());
      b.append("\n");
      
      b.append("CPU Cores    : ");
      b.append(vmDescriptor.getCpuCores());
      b.append("\n");
      
      b.append("Stored Path  : ");
      b.append(vmDescriptor.getStoredPath());
      b.append("\n");
      
      
      b.append("Roles        : ");
      for(String role: vmDescriptor.getVmConfig().getRoles()){
        b.append(role);
        b.append(",");
      }
      b.append("\n");
      
      b.append("Status       : ");
      b.append(vmStatus);
      b.append("\n");
      
      b.append("Heartbeat    : ");
      if(heartbeat){
        b.append("CONNECTED");
      } else{
        b.append("DISCONNECTED");
      }
      
      b.append("\n");
      
      
      b.append("------------------------------------------------------------------------\n");
      
      
      
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(ExceptionUtil.getStackTrace(e));
    }
    return b.toString();
  }
}