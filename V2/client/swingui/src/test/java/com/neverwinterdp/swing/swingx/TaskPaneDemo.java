package com.neverwinterdp.swing.swingx;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.text.html.HTMLDocument;

import org.jdesktop.application.Action;
import org.jdesktop.swingx.JXTaskPane;
import org.jdesktop.swingx.JXTaskPaneContainer;

public class TaskPaneDemo extends JPanel {
  private JXTaskPane systemGroup;
  private JXTaskPane officeGroup;
  private JXTaskPane seeAlsoGroup;
  private JXTaskPane detailsGroup;

  /**
   * main method allows us to run as a standalone demo.
   */
  public static void main(String[] args) {
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        JFrame frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(new TaskPaneDemo());
        frame.setPreferredSize(new Dimension(800, 600));
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
      }
    });
  }

  public TaskPaneDemo() {
    super(new BorderLayout());

    createTaskPaneDemo();

    bind();
  }

  private void createTaskPaneDemo() {
    JXTaskPaneContainer tpc = new JXTaskPaneContainer();

    // "System" GROUP
    systemGroup = new JXTaskPane("System");
    systemGroup.setName("systemGroup");
    tpc.add(systemGroup);

    // "Office" GROUP
    officeGroup = new JXTaskPane("Office");
    officeGroup.setName("officeGroup");
    tpc.add(officeGroup);

    // "SEE ALSO" GROUP and ACTIONS
    seeAlsoGroup = new JXTaskPane("See Also");
    seeAlsoGroup.setName("seeAlsoGroup");
    tpc.add(seeAlsoGroup);

    // "Details" GROUP
    detailsGroup = new JXTaskPane("Detail");
    detailsGroup.setName("detailsGroup");

    // TODO better injection for editor area
    JEditorPane area = new JEditorPane("text/html", "<html>");
    area.setName("detailsArea");

    area.setFont(UIManager.getFont("Label.font"));

    Font defaultFont = UIManager.getFont("Button.font");

    String stylesheet = "body { margin-top: 0; margin-bottom: 0; margin-left: 0; margin-right: 0; font-family: "
        + defaultFont.getName()
        + "; font-size: "
        + defaultFont.getSize()
        + "pt;  }"
        + "a, p, li { margin-top: 0; margin-bottom: 0; margin-left: 0; margin-right: 0; font-family: "
        + defaultFont.getName()
        + "; font-size: "
        + defaultFont.getSize()
        + "pt;  }";
    if (area.getDocument() instanceof HTMLDocument) {
      HTMLDocument doc = (HTMLDocument) area.getDocument();
      try {
        doc.getStyleSheet().loadRules(new java.io.StringReader(stylesheet),
                                      null);
      } catch (Exception e) {
        // TODO: handle exception
      }
    }
    area.setText("This is a test") ;
    detailsGroup.add(area);

    tpc.add(detailsGroup);

    add(new JScrollPane(tpc));
  }

  private void bind() {
    systemGroup.add(new AbstractAction("email") {
      public void actionPerformed(ActionEvent e) {
      }
    });
    systemGroup.add(new AbstractAction("delete") {
      public void actionPerformed(ActionEvent e) {
      }
    });

    officeGroup.add(new AbstractAction("write") {
      public void actionPerformed(ActionEvent e) {
      }
    });
    
    seeAlsoGroup.add(new AbstractAction("Browser") {
      public void actionPerformed(ActionEvent e) {
      }
    });
    seeAlsoGroup.add(new AbstractAction("Help") {
      public void actionPerformed(ActionEvent e) {
      }
    });
  }

  @Action
  public void email() {
  }

  @Action
  public void delete() {
  }

  @Action
  public void write() {
  }

  @Action
  public void exploreInternet() {
  }

  @Action
  public void help() {
  }
}
