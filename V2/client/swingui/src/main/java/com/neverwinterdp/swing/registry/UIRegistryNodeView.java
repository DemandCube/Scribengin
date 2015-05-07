package com.neverwinterdp.swing.registry;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JToolBar;

import com.neverwinterdp.swing.widget.JTabbedPaneUI;

@SuppressWarnings("serial")
public class UIRegistryNodeView extends JPanel {
  private String nodePath;
  private String nodeName ;
  private String label ;
  private JTabbedPaneUI jtabbedPane;
  
  public UIRegistryNodeView(String path, String nodeName) {
    this.nodePath = path ;
    this.nodeName = nodeName ;
    this.label = nodeName; 
    
    JToolBar toolbar = new JToolBar();
    toolbar.setFloatable(false);
    toolbar.add(new AbstractAction("Reload") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Delete") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Watch Create") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Watch Modify") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Watch Delete") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    setLayout(new BorderLayout());
    add(toolbar, BorderLayout.NORTH);
    jtabbedPane = new JTabbedPaneUI() ;
    jtabbedPane.withBottomTabPlacement();
    add(jtabbedPane, BorderLayout.CENTER);
    
    addView("Info", new UIRegistryNodeInfo(path), false) ;
  }
  
  public String getNodeName() { return this.nodeName ; }
  
  public String getNodePath() { return this.nodePath ; }
  
  public String getLabel() { return this.label ; }
  
  public void setLabel(String label) { this.label = label ; }
  
  public void addView(String label, JComponent view, boolean removable) {
    this.jtabbedPane.addTab(label, view, removable);
  }
}
