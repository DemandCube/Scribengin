package com.neverwinterdp.swing.console.robot;

import java.awt.Frame;

import javax.swing.JDialog;
import javax.swing.JPanel;

import org.fest.swing.core.GenericTypeMatcher;
import org.fest.swing.core.matcher.JButtonMatcher;
import org.fest.swing.fixture.ContainerFixture;
import org.fest.swing.fixture.DialogFixture;
import org.fest.swing.fixture.FrameFixture;
import org.fest.swing.fixture.JTableFixture;
import org.fest.swing.fixture.JTabbedPaneFixture ;

public class FrameUI {
  private FrameFixture fmatcher ; 
  
  public FrameUI(Frame frame) {
    this.fmatcher = new FrameFixture(frame) ;
  }
  
  public ContainerUI panel(String name) {
    return new ContainerUI(fmatcher.panel(name)) ;
  }
  
  public ContainerUI panel(final Class<? extends JPanel> type) {
    GenericTypeMatcher<JPanel> matcher = new GenericTypeMatcher<JPanel>(JPanel.class) {
      protected boolean isMatching(JPanel component) {
        if(!type.isInstance(component)) return false  ;
        return true;
      }
    } ;
    return new ContainerUI(fmatcher.panel(matcher)) ;
  }
  
  public ContainerUI dialog(String name) {
    return new ContainerUI(fmatcher.dialog(name)) ;
  }
  
  public TableUI table(String name) {
    return new TableUI(fmatcher.table(name)) ;
  }
  
  public TableUI table() {
    return new TableUI(fmatcher.table()) ;
  }
  
  public TabPaneUI tabbedPane(String name) {
    return new TabPaneUI(fmatcher.tabbedPane(name)) ;
  }

  public TabPaneUI tabbedPane() {
    return new TabPaneUI(fmatcher.tabbedPane()) ;
  }
  
  public ContainerUI dialog(final Class<? extends JDialog> type) {
    GenericTypeMatcher<JDialog> matcher = new GenericTypeMatcher<JDialog>(JDialog.class) {
      protected boolean isMatching(JDialog component) {
        if(!type.isInstance(component)) return false  ;
        return true;
      }
    } ;
    return new ContainerUI(fmatcher.dialog(matcher)) ;
  }
  
  public void destroy() {
    fmatcher.robot.cleanUpWithoutDisposingWindows() ;
  }
  
  static public class ContainerUI {
    ContainerFixture<?> matcher ;
    
    public ContainerUI(ContainerFixture<?> matcher) {
      this.matcher = matcher ;
    }
    
    public ContainerUI buttonClick(String text){
      matcher.button(JButtonMatcher.withText(text)).click() ;
      return this ;
    }
    
    public ContainerUI fieldSet(String name, String text){
      matcher.textBox(name).setText(text) ;
      return this ;
    }
    
    public ContainerUI dialogClose() {
      DialogFixture dFixture = (DialogFixture) matcher ;
      dFixture.close() ;
      return this ;
    }
  }
  
  static public class TableUI {
    JTableFixture matcher ;
    
    public TableUI(JTableFixture matcher) {
      this.matcher = matcher ;
    }
    
    public TableUI doubleClickRow(String text){
      int row = matcher.cell(text).row ;
      matcher.selectRows(row).doubleClick() ;
      return this ;
    }
  }
  
  static public class TabPaneUI {
    JTabbedPaneFixture matcher ;
    
    public TabPaneUI(JTabbedPaneFixture matcher) {
      this.matcher = matcher ;
    }
    
    public TabPaneUI clickTab(String text){
      matcher.selectTab(text).click() ;
      return this ;
    }
  }
}
