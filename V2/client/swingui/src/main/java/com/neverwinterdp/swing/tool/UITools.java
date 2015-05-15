package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.registry.UILogTree;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.JAccordionPanel;

@SuppressWarnings("serial")
public class UITools extends JPanel implements UILifecycle {
  private ToolsJAccordionPanel accordionPanel ;
 
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
      UIEmbeddedCluster embeddedCluster = new UIEmbeddedCluster();
      addBar("Embedded Cluster", embeddedCluster);
      addBar("Cluster Connection",  new UIClusterConnection());
      addBar("Registry Loggers", new UILogTree());
      addBar("Tests",            new UIDataflowTests());
      embeddedCluster.onActivate();
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
