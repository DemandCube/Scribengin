package com.neverwinterdp.swing.registry;

import java.awt.BorderLayout;
import java.awt.Component;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.JTabbedPaneUI;

@SuppressWarnings("serial")
public class UIRegistryNodeView extends JPanel {
  private String nodePath;
  private String nodeName ;
  private String label ;
  private JTabbedPaneUI jtabbedPane;
  private UILifecycle currentSelectTab ;
  
  public UIRegistryNodeView(String path, String nodeName) {
    this.nodePath = path ;
    this.nodeName = nodeName ;
    this.label = nodeName; 
    
    setLayout(new BorderLayout());
    jtabbedPane = new JTabbedPaneUI() ;
    jtabbedPane.addChangeListener(new ChangeListener() {
      public void stateChanged(ChangeEvent e) {
        int selectIdx = jtabbedPane.getJTabbedPane().getSelectedIndex() ;
        Component selComp = jtabbedPane.getJTabbedPane().getComponentAt(selectIdx);
        if(selComp instanceof UILifecycle) {
          try {
            if(currentSelectTab != null) {
              currentSelectTab.onDeactivate();
            }
            UILifecycle uiLifecycle = (UILifecycle) selComp ;
            uiLifecycle.onActivate();
            currentSelectTab = uiLifecycle ;
          } catch(Exception ex) {
            ex.printStackTrace();
            MessageUtil.handleError(ex);
          }
        }
      }
    });
    jtabbedPane.withBottomTabPlacement();
    add(jtabbedPane, BorderLayout.CENTER);
    
    addView("Info", new UIRegistryNodeInfo(path), false, false) ;
    jtabbedPane.setSelectedTab(0);
  }
  
  public String getNodeName() { return this.nodeName ; }
  
  public String getNodePath() { return this.nodePath ; }
  
  public String getLabel() { return this.label ; }
  
  public void setLabel(String label) { this.label = label ; }
  
  public void addView(String label, UILifecycle view, boolean removable) {
    jtabbedPane.addTab(label, (JComponent)view, removable, false);
  }
  
  public void addView(String label, UILifecycle view, boolean removable, boolean selected) {
    jtabbedPane.addTab(label, (JComponent)view, removable, selected);
  }
  
  public void setSelectedView(int idx) {
    jtabbedPane.setSelectedTab(idx);
  }
}
