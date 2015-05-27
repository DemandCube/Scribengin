package com.neverwinterdp.swing.es.plugin;

import java.awt.BorderLayout;
import java.awt.Component;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.JTabbedPaneUI;

@SuppressWarnings("serial")
public class UIESClusterControl extends JPanel  implements UILifecycle {
  private JTabbedPaneUI tabbedPaneUI ;
  private UILifecycle   currentSelectTab ;
  
  public UIESClusterControl() {
    setLayout(new BorderLayout());
  }
  
  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() {
    removeAll();
    tabbedPaneUI = new JTabbedPaneUI() ;
    tabbedPaneUI.addTab("Settings", new UIESClusterSetting(), false);
    tabbedPaneUI.addTab("Admin",    new UIESClusterAdmin(), false);

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
    tabbedPaneUI.setSelectedTab(0);
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
}