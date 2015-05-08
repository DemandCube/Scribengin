package com.neverwinterdp.swing.registry;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;

@SuppressWarnings("serial")
public class UIActivityQueueView extends UIActivityListView {

  public UIActivityQueueView(String activityRootPath, String path) {
    super(activityRootPath, path);
  }
  
  
  @Override
  protected List<Activity> getActivities(Registry registry) throws RegistryException {
    List<Activity> activities = registry.getChildrenAs(getListPath(), Activity.class) ;
    return activities ;
  }
}
