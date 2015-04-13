package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeFormatter;

public class ActiveActivityStepRegistryFormatter extends NodeFormatter{
  Node node;
  
  public ActiveActivityStepRegistryFormatter(Node n){
    this.node = n;
  }

  @Override
  public String getFormattedText() {
    return new ActiveActivityRegistryFormatter(this.node.getParentNode()
        .getParentNode().getParentNode()).getFormattedText();
  }
 

}
