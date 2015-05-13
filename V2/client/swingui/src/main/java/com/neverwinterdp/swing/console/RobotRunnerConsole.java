package com.neverwinterdp.swing.console;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.io.File;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JSplitPane;

import com.neverwinterdp.swing.ScriptRunner;
import com.neverwinterdp.swing.widget.fexplorer.FileTreeNode;
import com.neverwinterdp.swing.widget.fexplorer.FileTreePanel;

public class RobotRunnerConsole extends JDialog {  
  private ScriptWorkSpace   workspace ;
  
  public RobotRunnerConsole(Frame uiroot, boolean modal) throws Exception {
    super((JFrame) null, modal);
    workspace = new ScriptWorkSpace(uiroot) ;
    final FileTreePanel fileTreePanel = new FileTreePanel() ;
    Action openAction = new AbstractAction("Open") {
      public void actionPerformed(ActionEvent e) {
        FileTreeNode selectNode = fileTreePanel.getLastSelectedNode() ;
        File file = selectNode.getFile() ;
        if(file.exists() && file.isFile()) {
          workspace.openFile(file.getAbsolutePath());
        }
      }      
    };
    fileTreePanel.register(openAction, true);
    fileTreePanel.register(fileTreePanel.createDeteleAction(), false);
    fileTreePanel.register(fileTreePanel.createRefreshAction(), false);
    
    JSplitPane splitPane =
      new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, fileTreePanel, workspace);
    splitPane.setDividerLocation(250);
    splitPane.setOneTouchExpandable(true);
    
    getContentPane().add(splitPane);

   
    pack();
    setLocationRelativeTo(null);
    setVisible(true);
  }

  public ScriptRunner getScriptRunner() {
    return workspace.getScriptRunner() ;
  }
}