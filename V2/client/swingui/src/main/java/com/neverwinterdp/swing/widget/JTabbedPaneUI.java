package com.neverwinterdp.swing.widget;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.event.ChangeListener;


public class JTabbedPaneUI  extends JPanel {
  private static final long serialVersionUID = 1L;

  private JTabbedPane tabbedPane ;
  
  public JTabbedPaneUI() {
    tabbedPane = new JTabbedPane();
    tabbedPane.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);
    
    setLayout(new BorderLayout()) ;
    add(tabbedPane, BorderLayout.CENTER) ;
  }
  
  public JTabbedPane getJTabbedPane() { return this.tabbedPane; }
  
  public JTabbedPaneUI withVerticalTabPlacement() {
    tabbedPane.setTabPlacement(JTabbedPane.LEFT);
    return this ;
  }
  
  public JTabbedPaneUI withBottomTabPlacement() {
    tabbedPane.setTabPlacement(JTabbedPane.BOTTOM);
    return this ;
  }
  
  public void addTab(String title, JComponent jpanel, boolean closable) {
    for(int i = 0; i < tabbedPane.getTabCount(); i++) {
      String checkTitle = tabbedPane.getTitleAt(i) ;
      if(title.equals(checkTitle)) {
        tabbedPane.remove(i) ;
        break ;
      }
    }
    int index = tabbedPane.getTabCount() ;
    tabbedPane.add(title, jpanel);
    if(closable) {
      ClosableTabButton ctBtn = new ClosableTabButton(tabbedPane) ;
      tabbedPane.setTabComponentAt(index, ctBtn);
    }
    tabbedPane.setSelectedIndex(index);
  }
  
  public void setSelectedTab(int idx) {
    tabbedPane.setSelectedIndex(idx) ;
  }
  
  public void renameTab(String newTitle, Component comp) {
    for(int i = 0; i < getTabCount(); i++) {
      Component tab = getTabAt(i) ;
      if(tab == comp) {
        tabbedPane.setTitleAt(i, newTitle) ;
      }
    }
  }
  
  public void addAddButton(ActionListener listener) {
    int selTab = tabbedPane.getSelectedIndex() ;
    addTab("", null, false) ;
    JButton jbutton = new JButton("+") ;
    if(listener != null) {
      jbutton.addActionListener(listener) ;
    }
    tabbedPane.setTabComponentAt(tabbedPane.getTabCount() - 1, jbutton);
    tabbedPane.setSelectedIndex(selTab);
  }
  
  public int getTabCount() { return tabbedPane.getTabCount() ; }

  public Component getTabAt(int idx) {
    return tabbedPane.getComponentAt(idx) ;
  }
  
  public void removeTabAt(int idx) {
    tabbedPane.remove(idx) ;
  }
  
  public void removeTab(Component component) {
    int idx = tabbedPane.indexOfComponent(component) ;
    tabbedPane.removeTabAt(idx) ;
  }
  
  public void addChangeListener(ChangeListener listener) {
    tabbedPane.addChangeListener(listener) ;
  }
}
