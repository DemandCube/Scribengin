package com.neverwinterdp.vm.command;

import java.util.List;

import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.vm.VM;

public class VMCommandWatcher extends NodeWatcher {
  private VM vm;
  
  public VMCommandWatcher(VM vm) {
    this.vm = vm ;
  }
  
  @Override
  synchronized public void onEvent(NodeEvent event) {
    if(NodeEvent.Type.CHILDREN_CHANGED == event.getType()) {
      String path = event.getPath();
      try {
        Registry registry = vm.getVMRegistry().getRegistry() ;
        registry.watchChildren(path, this);
        List<String> cmdPathNames = registry.getChildren(path, true);
        for(String cmdPathName : cmdPathNames) {
          String cmdPath = path + "/" + cmdPathName;
          CommandPayload payload = registry.getDataAs(cmdPath, CommandPayload.class);
          Command command = payload.getCommand();
          CommandResult<?> result = null ;
          try {
            result = command.execute(vm);
          } catch(Throwable t) {
            result = new CommandResult() ;
            result.setErrorStacktrace(ExceptionUtil.getStackTrace(t));
          }
          payload.setResult(result);
          registry.setData(cmdPath, payload);
        }
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
  
}
