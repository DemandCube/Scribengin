package com.neverwinterdp.registry.util;

import java.io.IOException;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;

public interface NodeFormater {
  public String getFormattedText(Node node, String indent) ;
  
  static public class NodeDataFormater implements NodeFormater {
    @Override
    public String getFormattedText(Node node, String indent) {
      StringBuilder b = new StringBuilder() ;
      try {
        String text = new String(node.getData());
        if(indent != null && indent.length() > 0) {
          String[] array = text.split("\n") ;
          for(int i = 0; i < array.length; i++) {
            b.append(indent + array[i]);
          }
        } else {
          b.append(text);
        }
      } catch (RegistryException e) {
        e.printStackTrace();
      }
      return b.toString();
    }
  }
  
  static public class NodeDumpFormater implements NodeFormater {
    private Node nodeToDump = null;
      
    public NodeDumpFormater() {
    }
    
    public NodeDumpFormater(Node nodeToDump) {
      this.nodeToDump = nodeToDump ;
    }
    
    @Override
    public String getFormattedText(Node node, String indent) {
      StringBuilder b = new StringBuilder() ;
      try {
        if(nodeToDump != null) nodeToDump.dump(b, indent);
        else node.dump(b, indent);
      } catch (RegistryException | IOException e) {
        e.printStackTrace();
      }
      return b.toString();
    }
  }
}
