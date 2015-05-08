package com.neverwinterdp.swing.vm;

import com.neverwinterdp.swing.registry.UIRegistryNodeView;
import com.neverwinterdp.swing.registry.UIRegistryTree;

@SuppressWarnings("serial")
public class UIVMTree extends UIRegistryTree {
  private RegistryTreeNodePathMatcher ignoreNodeFilter ;
  private RegistryTreeNodePathMatcher listVMNodeMatcher ;
  
  public UIVMTree() throws Exception {
    super("/vm/instances", "VM");
    ignoreNodeFilter = new RegistryTreeNodePathMatcher() ;
    ignoreNodeFilter.add("/vm/.*/all/.*/status");
    ignoreNodeFilter.add("/vm/.*/all/.*/commands");
    
    listVMNodeMatcher = new RegistryTreeNodePathMatcher() ;
    listVMNodeMatcher.add("/vm/instances/active");
    listVMNodeMatcher.add("/vm/instances/history");
    listVMNodeMatcher.add("/vm/instances/all");
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
    if(listVMNodeMatcher.matches(node)) {
      view.addView("VM List", new UIVMListView(node.getNodePath()), false) ;
    }
  }
  
  public RegistryTreeNode onCustomTreeNode(RegistryTreeNode node) {
    if(ignoreNodeFilter.matches(node)) return null; 
    return node ;
  }
}
