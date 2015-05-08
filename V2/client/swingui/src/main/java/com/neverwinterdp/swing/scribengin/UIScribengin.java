package com.neverwinterdp.swing.scribengin;

import java.util.List;

import javax.swing.border.EmptyBorder;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.JAccordionPanel;

@SuppressWarnings("serial")
public class UIScribengin extends JAccordionPanel implements UILifecycle {
  
  public UIScribengin() throws Exception {
    setOpaque(false);
    setBorder(new EmptyBorder(0, 0, 5, 0) );

    addBar("Dataflows", new UIDataflowTree());
    addBar("Masters", getDummyPanel("Masters"));
    addBar("Events", getDummyPanel("Events"));
    addBar("Activities", getDummyPanel("Activities"));
    setVisibleBar(2);
  }

  @Override
  public void onInit() throws Exception {
    List<UILifecycle> descendants = SwingUtil.findDescentdantsOfType(this, UILifecycle.class, false) ;
    for(UILifecycle sel : descendants) sel.onInit();;
  }

  @Override
  public void onDestroy() throws Exception {
    List<UILifecycle> descendants = SwingUtil.findDescentdantsOfType(this, UILifecycle.class, false) ;
    for(UILifecycle sel : descendants) sel.onDestroy();
  }

  @Override
  public void onActivate() throws Exception {
    setSize(getParent().getSize());
    List<UILifecycle> descendants = SwingUtil.findDescentdantsOfType(this, UILifecycle.class, false) ;
    for(UILifecycle sel : descendants) sel.onActivate();
  }

  @Override
  public void onDeactivate() throws Exception {
    List<UILifecycle> descendants = SwingUtil.findDescentdantsOfType(this, UILifecycle.class, false) ;
    for(UILifecycle sel : descendants) sel.onDeactivate();
  }
}
