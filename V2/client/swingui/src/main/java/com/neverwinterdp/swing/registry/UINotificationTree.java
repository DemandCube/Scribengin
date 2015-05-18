package com.neverwinterdp.swing.registry;

@SuppressWarnings("serial")
public class UINotificationTree extends UIRegistryTree {
  
  private RegistryTreeNodePathMatcher notificationNodeMatcher ;
  
  
  public UINotificationTree(String path) {
    super(path, "Notifications");
    notificationNodeMatcher = new RegistryTreeNodePathMatcher() ;
    notificationNodeMatcher.add(path + "/.*/.*-events");
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
    if(notificationNodeMatcher.matches(node)) {
      view.addView("Notifications", new UINotificationView(node.getNodePath()), false) ;
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
