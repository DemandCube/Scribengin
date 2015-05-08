package com.neverwinterdp.swing;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import org.jdesktop.swingx.JXMultiSplitPane;
import org.jdesktop.swingx.MultiSplitLayout;

import com.neverwinterdp.swing.console.UIShellConsole;

@SuppressWarnings("serial")
public class UIMain extends JPanel {
  private UIControl      uiControl;
  private UIWorkspace    uiWorkspace;
  private UIShellConsole uiShellConsole;

  public UIMain() throws Exception {
    initMainUI();
    bind();
  }

  public UIControl getUiControl() { return uiControl; }

  public UIWorkspace getUiWorkspace() { return uiWorkspace; }

  public UIShellConsole getUiShellConsole() { return uiShellConsole; }

  // TODO enable resource injection for the components in this demo
  private void initMainUI() throws Exception {
    setLayout(new BorderLayout());
    JXMultiSplitPane msp = new JXMultiSplitPane();
    String layoutDef = 
        "(COLUMN " + 
        "  (ROW weight=0.65 " + 
        "    (LEAF name=main.ui.control weight=0.2) " + 
        "    (LEAF name=main.ui.workspace weight=0.8)" + 
        "  )" + 
        "  (LEAF name=main.ui.console weight=0.35)" + 
        ")";
    
    MultiSplitLayout.Node modelRoot = MultiSplitLayout.parseModel(layoutDef);
    msp.getMultiSplitLayout().setModel(modelRoot);
    uiControl = new UIControl() ;
    msp.add(uiControl,      "main.ui.control");
   
    uiWorkspace = new UIWorkspace();
    msp.add(uiWorkspace,    "main.ui.workspace");
    
    uiShellConsole = new UIShellConsole() ;
    msp.add(uiShellConsole, "main.ui.console");
    //msp.setDividerSize(2);
    
    // ADDING A BORDER TO THE MULTISPLITPANE CAUSES ALL SORTS OF ISSUES
    msp.setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));
    add(msp, BorderLayout.CENTER);
  }

  private void bind() {
    // no bindings
  }
  
  /**
   * main method allows us to run as a standalone demo.
   */
  public static void main(String[] args) throws Exception {
    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        try {
          JFrame frame = new JFrame();
          frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
          frame.getContentPane().add(new UIMain());
          frame.setPreferredSize(new Dimension(1200, 800));
          frame.pack();
          frame.setLocationRelativeTo(null);
          frame.setVisible(true);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }
}