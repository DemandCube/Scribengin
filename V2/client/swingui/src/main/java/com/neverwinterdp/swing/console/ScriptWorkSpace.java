package com.neverwinterdp.swing.console;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Frame;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JSplitPane;

import com.neverwinterdp.swing.ScriptRunner;
import com.neverwinterdp.swing.console.robot.FrameUI;

public class ScriptWorkSpace extends JPanel {
  private ScriptRunner      scriptRunner;
  private UIShellConsole       scriptShell;
  private ScriptEditor      scriptEditor ;
  
  public ScriptWorkSpace(Frame uiRoot) throws Exception {
    setLayout(new BorderLayout());
    setPreferredSize(new Dimension(600, 500));
    scriptEditor = new ScriptEditor();
    scriptShell = new UIShellConsole();

    JSplitPane splitPane =
      new JSplitPane(JSplitPane.VERTICAL_SPLIT, scriptEditor, scriptShell);
    splitPane.setDividerLocation(350);
    splitPane.setOneTouchExpandable(true);

    add(splitPane, BorderLayout.CENTER);

    Map<String, Object> ctx = new HashMap<String, Object>() ;
    ctx.put("console", scriptShell) ;
    ctx.put("jvm", new JVMEnv()) ;
    ctx.put("uiroot", uiRoot) ;
    //ctx.put("frameui", new FrameUI(uiRoot)) ;
    scriptRunner = new ScriptRunner(".", ctx);
  }

  public ScriptRunner getScriptRunner() { return this.scriptRunner ; }

  public UIShellConsole getScriptShell() { return this.scriptShell ; }
  
  public void openFile(String file) {
    this.scriptEditor.open(file) ;
  }
}