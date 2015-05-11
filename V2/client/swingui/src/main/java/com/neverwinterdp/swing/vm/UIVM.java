package com.neverwinterdp.swing.vm;

import java.util.List;

import javax.swing.border.EmptyBorder;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.JAccordionPanel;

public class UIVM extends JAccordionPanel implements UILifecycle {
  public UIVM() throws Exception {
    setOpaque(false);
    setBorder(new EmptyBorder(0, 0, 5, 0) );
    
    addBar("VM", new UIVMTree());
    addBar("Masters", getDummyPanel("Masters"));
    addBar("Events", getDummyPanel("Events"));
    addBar("Activities", getDummyPanel("Activities"));
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
