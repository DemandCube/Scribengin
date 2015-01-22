package com.neverwinterdp.vm.command;

import java.util.List;

import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.vm.VM;

public class VMCommandWatcher extends NodeWatcher {
  private VM vm;
  
  public VMCommandWatcher(VM vm) {
    this.vm = vm ;
  }
  
  private long seqTracker = 0 ;
  
  @Override
  synchronized public void onEvent(NodeEvent event) {
    if(NodeEvent.Type.CHILDREN_CHANGED == event.getType()) {
      String path = event.getPath();
      try {
        Registry registry = vm.getVMRegistry().getRegistry() ;
        registry.watchChildren(path, this);
        List<String> cmdPathNames = registry.getChildren(path, true);
        //System.out.println("path = " + path + ", children = " + cmdPathNames.size());
        for(String cmdPathName : cmdPathNames) {
          String seq = cmdPathName.substring("command-".length()) ;
          long seqId = Long.parseLong(seq);
          //System.out.println("  cmdPathName " + cmdPathName);
          if(seqId == seqTracker++) {
            String cmdPath = path + "/" + cmdPathName;
            CommandPayload payload = registry.getDataAs(cmdPath, CommandPayload.class);
            Command command = payload.getCommand();
            CommandResult<?> result = command.execute(vm);
            payload.setResult(result);
            registry.setData(cmdPath, payload);
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
  
}
