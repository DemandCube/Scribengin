package com.neverwinterdp.swing.widget;

import javax.swing.JLabel;

import com.neverwinterdp.swing.util.ResourceManager;

public class LocalizableJLabel extends JLabel implements Localizable {
  private String key ;
  
  public LocalizableJLabel(ResourceManager resManager, String key) {
    this.key = key ;
    update(resManager) ;
  }
  
  public String getKey() { return key; }

  public void update(ResourceManager resManager) {
    setText(resManager.get(key)) ;
  }

}
