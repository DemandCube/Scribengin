package com.neverwinterdp.vm.client.shell;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;

public class RegistryCommand extends Command {
  public RegistryCommand() {
    add("dump", new Dump()) ;
  }
  
  static public class Dump extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      Registry registry = vmClient.getRegistry();
      List<String> nodes = registry.getChildren("/");
      for(String node : nodes) {
        dump("/", node, registry, shell.console(), "");
      }
    }
    
    private void dump(String parent, String node, Registry registry, Console console, String indentation) throws IOException, RegistryException {
      String path = parent + "/" + node;
      if("/".equals(parent)) path = "/" + node;
      byte[] data = registry.getData(path);
      String stringData = "";
      if(data != null && data.length > 0) {
        stringData = " - " + new String(data);
        stringData = stringData.replace("\r\n", " ");
        stringData = stringData.replace("\n", " ");
        if(stringData.length() > 80) {
          stringData = stringData.substring(0, 80);
        }
      }
      console.println(indentation + node + stringData);
      List<String> children = registry.getChildren(path);
      for(String child : children) {
        dump(path, child, registry, console, indentation + "  ");
      }
    }
  }
}
