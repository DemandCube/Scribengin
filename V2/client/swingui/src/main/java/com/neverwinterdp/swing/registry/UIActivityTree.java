package com.neverwinterdp.swing.registry;

@SuppressWarnings("serial")
public class UIActivityTree extends UIRegistryTree {
  private RegistryTreeNodePathMatcher activityNodeMatcher ;
  private RegistryTreeNodePathMatcher activityListMatcher ;
  private RegistryTreeNodePathMatcher activityQueueMatcher ;
  private RegistryTreeNodePathMatcher ignoreNodeMatcher ;
  
  public UIActivityTree(String path) throws Exception {
    super(path, "Activities");
    activityNodeMatcher = new RegistryTreeNodePathMatcher() ;
    activityNodeMatcher.add(path + "/(active|history|all)/.*");
    
    activityListMatcher = new RegistryTreeNodePathMatcher() ;
    activityListMatcher.add(path + "/(active|history|all)");
    
    activityQueueMatcher = new RegistryTreeNodePathMatcher() ;
    activityQueueMatcher.add(path + "/queue");
    
    ignoreNodeMatcher = new RegistryTreeNodePathMatcher() ;
    ignoreNodeMatcher.add(path + "/activity-id-tracker");
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
    if(activityNodeMatcher.matches(node)) {
      view.addView("Activty", new UIActivityView(getRootPath()), false) ;
//      view.addView("Activity Steps", new UIActivityStepsView(getRootPath(), node.getNodeName()), false) ;
    } else if(activityListMatcher.matches(node)) {
      view.addView("Activities", new UIActivityView(getRootPath()), false) ;
    } else if(activityQueueMatcher.matches(node)) {
      view.addView("Queue Activities", new UIActivityQueueView(getRootPath(), node.getNodePath()), false) ;
    }
  }
  
  public RegistryTreeNode onCustomTreeNode(RegistryTreeNode node) {
    if(ignoreNodeMatcher.matches(node)) return null ;
    if(activityNodeMatcher.matches(node)) {
      node.setAllowsChildren(false);
    }
    return node ;
  }
}
