package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;

import javax.swing.JPanel;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.widget.JAccordionPanel;

@SuppressWarnings("serial")
public class UITools extends JPanel implements UILifecycle {
  private JAccordionPanel accordionPanel ;
 
  public UITools() {
    setLayout(new BorderLayout()) ;
  }

  @Override
  public void onInit() {
  }

  @Override
  public void onDestroy() {
  }

  @Override
  public void onActivate() {
    removeAll();
    accordionPanel = new JAccordionPanel();
    accordionPanel.addBar("Cluster Launcher", new UIClusterLauncher());
    accordionPanel.addBar("script", JAccordionPanel.getDummyPanel("Script"));
    accordionPanel.setVisibleBar(2);
    add(accordionPanel, BorderLayout.CENTER);
  }

  @Override
  public void onDeactivate() {
    removeAll();
  }
}
