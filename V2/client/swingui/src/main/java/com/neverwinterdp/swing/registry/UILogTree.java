package com.neverwinterdp.swing.registry;

@SuppressWarnings("serial")
public class UILogTree extends UIRegistryTree {
  
  private RegistryTreeNodePathMatcher loggerNodeMatcher ;
  
  
  public UILogTree() {
    super("/logs", "Logs");
    loggerNodeMatcher = new RegistryTreeNodePathMatcher() ;
    loggerNodeMatcher.add("/logs/.*/.*-logger");
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
    if(loggerNodeMatcher.matches(node)) {
      view.addView("Logs", new UILogView(node.getNodePath()), false) ;
    }
    view.setSelectedView(0);
  }
  
  public RegistryTreeNode onCustomTreeNode(RegistryTreeNode node) {
//    if(loggerNodeMatcher.matches(node)) {
//      node.setAllowsChildren(false);
//    }
    return node ;
  }
}
