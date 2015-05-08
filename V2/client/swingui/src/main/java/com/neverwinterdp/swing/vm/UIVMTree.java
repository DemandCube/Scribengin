package com.neverwinterdp.swing.vm;

import com.neverwinterdp.swing.registry.UIRegistryNodeView;
import com.neverwinterdp.swing.registry.UIRegistryTree;

@SuppressWarnings("serial")
public class UIVMTree extends UIRegistryTree {
  private RegistryTreeNodePathMatcher listVMNodeMatcher ;
  private RegistryTreeNodePathMatcher vmVMNodeMatcher ;
  
  public UIVMTree() throws Exception {
    super("/vm/instances", "VM");
    listVMNodeMatcher = new RegistryTreeNodePathMatcher() ;
    listVMNodeMatcher.add("/vm/instances/(active|history|all)");
    
    vmVMNodeMatcher = new RegistryTreeNodePathMatcher() ;
    vmVMNodeMatcher.add("/vm/instances/(active|history|all)/.*");
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
    if(listVMNodeMatcher.matches(node)) {
      view.addView("VM List", new UIVMListView(node.getNodePath()), false) ;
    } else if(vmVMNodeMatcher.matches(node)) {
      view.addView("VM", new UIVMView(node.getNodeName()), false) ;
    }
  }
  
  public RegistryTreeNode onCustomTreeNode(RegistryTreeNode node) {
    if(vmVMNodeMatcher.matches(node)) {
      node.setAllowsChildren(false);
    }
    return node ;
  }
}
