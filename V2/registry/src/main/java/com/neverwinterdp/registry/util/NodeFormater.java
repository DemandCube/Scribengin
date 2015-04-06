package com.neverwinterdp.registry.util;

import java.io.IOException;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.ExceptionUtil;

abstract public class NodeFormater {
  String indent = "" ;
  
  public NodeFormater setIndent(String indent) {
    this.indent = indent ;
    return this;
  }
  
  abstract public String getFormattedText() ;
  
  static public class NodeDataFormater extends NodeFormater {
    private Node node ;
    
    public NodeDataFormater(Node node, String indent) {
      this.node = node ;
      setIndent(indent);
    }
    
    @Override
    public String getFormattedText() {
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
  
  static public class NodeDumpFormater extends NodeFormater {
    private Node nodeToDump = null;
      
    public NodeDumpFormater(Node nodeToDump, String indent) {
      this.nodeToDump = nodeToDump ;
      setIndent(indent);
    }
    
    @Override
    public String getFormattedText() {
      StringBuilder b = new StringBuilder() ;
      try {
        nodeToDump.dump(b, indent);
      } catch (RegistryException | IOException e) {
        b.append(ExceptionUtil.getStackTrace(e));
      }
      return b.toString();
    }
  }
}
