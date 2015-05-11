package com.neverwinterdp.swing.swingx;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import org.jdesktop.swingx.JXMultiSplitPane;
import org.jdesktop.swingx.MultiSplitLayout;

@SuppressWarnings("serial")
public class MultiSplitPaneDemo extends JPanel {

  /**
   * main method allows us to run as a standalone demo.
   */
  public static void main(String[] args) {
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        JFrame frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(new MultiSplitPaneDemo());
        frame.setPreferredSize(new Dimension(800, 600));
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
      }
    });
  }

  public MultiSplitPaneDemo() {
    createMultiSplitPaneDemo();
    bind();
  }

  // TODO enable resource injection for the components in this demo
  private void createMultiSplitPaneDemo() {
    setLayout(new BorderLayout());

    JXMultiSplitPane msp = new JXMultiSplitPane();

    String layoutDef = 
        "(COLUMN " + 
        "  (ROW weight=0.8 " + 
        "    (COLUMN weight=0.25 " +
        "      (LEAF name=left.top weight=0.5) " +
        "      (LEAF name=left.middle weight=0.5) " + 
        "    )" +
        "    (LEAF name=editor weight=0.75)" + 
        "  )" + 
        "  (LEAF name=bottom weight=0.2)" + 
        ")";
    
    MultiSplitLayout.Node modelRoot = MultiSplitLayout.parseModel(layoutDef);
    msp.getMultiSplitLayout().setModel(modelRoot);

    msp.add(new JButton("Left Top"), "left.top");
    msp.add(new JButton("Left Middle"), "left.middle");
    msp.add(new JButton("Editor"), "editor");
    msp.add(new JButton("Bottom"), "bottom");

    // ADDING A BORDER TO THE MULTISPLITPANE CAUSES ALL SORTS OF ISSUES
    msp.setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));
    add(msp, BorderLayout.CENTER);
  }

  private void bind() {
    // no bindings
  }
}