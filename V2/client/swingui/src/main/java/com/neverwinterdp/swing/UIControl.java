package com.neverwinterdp.swing;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.neverwinterdp.swing.registry.UIRegistryTree;
import com.neverwinterdp.swing.tool.UITools;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.JTabbedPaneUI;

@SuppressWarnings("serial")
public class UIControl extends JPanel {
  private JTabbedPaneUI tabbedPaneUI ;
  private UILifecycle   currentSelectTab ;
  
  public UIControl() throws Exception {
    tabbedPaneUI = new JTabbedPaneUI() ;
    
    tabbedPaneUI.withVerticalTabPlacement();
    tabbedPaneUI.addTab("tools", new UITools(), false);
    tabbedPaneUI.addTab("registry", new UIRegistryTree(), false);
    tabbedPaneUI.addTab("vm", new JButton("Vm"), false);
    tabbedPaneUI.addTab("scribengin", new JButton("Scribengin"), false);
    tabbedPaneUI.addTab("Kafka", new JButton("Kafka"), false);
    tabbedPaneUI.addTab("Zookeeper", new JButton("Zookeeper"), false);
    tabbedPaneUI.addTab("Hadoop", new JButton("Hadoop"), false);
    tabbedPaneUI.setAutoscrolls(true);
    
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
            MessageUtil.handleError(ex);
          }
        }
      }
    });
    tabbedPaneUI.setSelectedTab(0);
    
    setLayout(new BorderLayout());
    add(tabbedPaneUI, BorderLayout.CENTER);
    setPreferredSize(new Dimension(150, 300));
  }

}
