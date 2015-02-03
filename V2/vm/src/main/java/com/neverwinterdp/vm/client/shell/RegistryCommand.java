package com.neverwinterdp.vm.client.shell;

import java.io.IOException;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;

public class RegistryCommand extends Command {
  public RegistryCommand() {
    add("dump", new Dump()) ;
  }
  
  static public class Dump extends SubCommand {
    @Parameter(names = "--path", description = "The path to dump, the default path is the root /")
    private String path = "/";
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      Registry registry = vmClient.getRegistry();
      List<String> nodes = registry.getChildren(path);
      shell.console().println(path);
      for(String node : nodes) {
        dump(path, node, registry, shell.console(), "  ");
      }
    }
    
    private void dump(String parent, String node, Registry registry, Console console, String indentation) throws IOException, RegistryException {
      //During the recursive traverse, a node can be added or removed by the other process
      //So we can ignore all the No node exists exception
      String path = parent + "/" + node;
      if("/".equals(parent)) path = "/" + node;
      byte[] data = {};
      try {
        data = registry.getData(path);
      } catch(RegistryException ex) {
      }
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
      List<String > children = null ;
      try {
        children = registry.getChildren(path);
      } catch(RegistryException ex) {
      }
      if(children != null) {
        for(String child : children) {
          dump(path, child, registry, console, indentation + "  ");
        }
      }
    }
  }
}
