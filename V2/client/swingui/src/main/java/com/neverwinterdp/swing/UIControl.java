package com.neverwinterdp.swing;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.neverwinterdp.swing.registry.UIRegistryTree;
import com.neverwinterdp.swing.scribengin.UIScribengin;
import com.neverwinterdp.swing.server.UIServers;
import com.neverwinterdp.swing.tool.UITools;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.vm.UIVM;
import com.neverwinterdp.swing.widget.JTabbedPaneUI;

@SuppressWarnings("serial")
public class UIControl extends JPanel {
  private JTabbedPaneUI tabbedPaneUI ;
  private UILifecycle   currentSelectTab ;
  
  public UIControl() throws Exception {
    setLayout(new BorderLayout());
    
    tabbedPaneUI = new JTabbedPaneUI() ;
    tabbedPaneUI.addTab("Tools", new UITools(), false);
    tabbedPaneUI.addTab("Registry", new UIRegistryTree("/", "/"), false);
    tabbedPaneUI.addTab("VM", new UIVM(), false);
    tabbedPaneUI.addTab("Scribengin", new UIScribengin(), false);
    tabbedPaneUI.addTab("Servers", new UIServers(), false);
    
    tabbedPaneUI.withVerticalTabPlacement();
    
    tabbedPaneUI.addChangeListener(new ChangeListener() {
      public void stateChanged(ChangeEvent e) {
        int selectIdx = tabbedPaneUI.getJTabbedPane().getSelectedIndex() ;
        Component selComp = tabbedPaneUI.getJTabbedPane().getComponentAt(selectIdx);
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
    
    add(tabbedPaneUI, BorderLayout.CENTER);
    this.setPreferredSize(new Dimension(200, 450));
    tabbedPaneUI.setSelectedTab(0);
  }
  
  public void fitSize() {
    tabbedPaneUI.setSize(getParent().getSize());
  }
}
