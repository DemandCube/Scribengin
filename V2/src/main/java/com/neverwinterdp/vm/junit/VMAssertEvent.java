package com.neverwinterdp.vm.junit;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.junit.AssertEvent;

public class VMAssertEvent extends AssertEvent {
  static public enum Attr { vmdescriptor, vmstatus, heartbeat, master_leader}
  
  final static public String VM_STATUS          = "vm-status" ;
  final static public String VM_HEARTBEAT       = "vm-heartbeat" ;
  final static public String VM_MASTER_ELECTION = "vm-master-election" ;
  
  public VMAssertEvent(String name, NodeEvent event) {
    super(name, event);
  }
  
  public void attr(Attr attr, Object value) {
    attr(attr.toString(), value);
  }
  
  public <T> T attr(Attr attr) {
    return (T) attr(attr.toString());
  }
}