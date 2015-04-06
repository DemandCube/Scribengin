package com.neverwinterdp.vm.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormater;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;

//TODO: figure out a way to pretty print the vm information data
//The class should print out the data in the VMDescriptor, the status
public class VMRegistryFormater extends NodeFormater {
  private Node vmNode ;
  
  public VMRegistryFormater(Node vmNode) {
    this.vmNode = vmNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      VMDescriptor vmDescriptor = vmNode.getDataAs(VMDescriptor.class);
      VMStatus vmStatus = vmNode.getChild("status").getDataAs(VMStatus.class);
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(ExceptionUtil.getStackTrace(e));
    }
    return b.toString();
  }
}