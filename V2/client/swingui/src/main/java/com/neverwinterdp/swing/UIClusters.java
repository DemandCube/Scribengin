package com.neverwinterdp.swing;

import java.awt.BorderLayout;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.neverwinterdp.swing.es.plugin.UIESClusterControl;
import com.neverwinterdp.swing.es.plugin.UIESClusterSetting;
import com.neverwinterdp.swing.scribengin.UIScribenginCluster;
import com.neverwinterdp.swing.scribengin.dataflow.UIDataflowTests;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.JAccordionPanel;

@SuppressWarnings("serial")
public class UIClusters extends JPanel implements UILifecycle {
  private ToolsJAccordionPanel accordionPanel ;
 
  public UIClusters() {
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
  public void onActivate()  {
    removeAll();
    accordionPanel = new ToolsJAccordionPanel();
    add(accordionPanel, BorderLayout.CENTER);
  }

  @Override
  public void onDeactivate() {
    removeAll();
  }
  
  static public class ToolsJAccordionPanel extends JAccordionPanel {
    private UILifecycle currentSelectPanel = null ;

    public ToolsJAccordionPanel() {
      UIESClusterControl uiESClusterControl = new UIESClusterControl();
      addBar("ElasticSearch Cluster", uiESClusterControl);
      addBar("Scribengin Cluster", new UIScribenginCluster());
      addBar("Dataflow Tests",  new UIDataflowTests());
      onSelectBarInfo(uiESClusterControl);
    }
    
    @Override
    public void onSelectBarInfo(JComponent newPanel) {
      try {
        if(currentSelectPanel != null) currentSelectPanel.onDeactivate();
        currentSelectPanel = null ;
        if(newPanel instanceof UILifecycle) {
          currentSelectPanel = (UILifecycle) newPanel;
          currentSelectPanel.onActivate();
          newPanel.revalidate();
        }
      } catch(Exception ex) {
        MessageUtil.handleError(ex);
      }
    }
  }
}
