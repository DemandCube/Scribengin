package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;

import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.widget.JAccordionPanel;

@SuppressWarnings("serial")
public class UITools extends JPanel implements UILifecycle {
  private JAccordionPanel accordionPanel ;
 
  public UITools() {
    setLayout(new BorderLayout()) ;
    setOpaque(false);
    setBorder(new EmptyBorder(0, 0, 5, 0) );
  }

  @Override
  public void onInit() {
  }

  @Override
  public void onDestroy() {
  }

  @Override
  public void onActivate() {
    System.err.println("call activate UITools.........................");
    removeAll();
    accordionPanel = new JAccordionPanel();
    accordionPanel.addBar("Embedded Cluster", new UIEmbeddedCluster());
    accordionPanel.addBar("Remote Cluster", new UIRemoteCluster());
    accordionPanel.addBar("Tests", new UITests());
    add(accordionPanel, BorderLayout.CENTER);
  }

  @Override
  public void onDeactivate() {
    removeAll();
  }
}
