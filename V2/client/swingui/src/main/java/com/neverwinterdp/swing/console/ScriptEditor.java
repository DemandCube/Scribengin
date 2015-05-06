package com.neverwinterdp.swing.console;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;

import javax.swing.AbstractAction;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JToolBar;

import com.neverwinterdp.swing.util.IOUtil;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.ClosableTabButton;

public class ScriptEditor extends JPanel {
  private JTabbedPane       tabbedPane ;
  
  public ScriptEditor() {
    setLayout(new BorderLayout());
     
    JToolBar toolBar = new JToolBar();
    add(toolBar, BorderLayout.NORTH);

    toolBar.add(new AbstractAction("New") {
      public void actionPerformed(ActionEvent e) {
        JTextAreaEditor editor = new JTextAreaEditor(null) ;
        addTabView(editor.label, new JScrollPane(editor));
      }
    });

    toolBar.add(new AbstractAction("Save") {
      public void actionPerformed(ActionEvent e) { save() ; }
    });

    toolBar.add(new AbstractAction("Run") {
      public void actionPerformed(ActionEvent e) { run() ; }
    });

    tabbedPane = new JTabbedPane();
    tabbedPane.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);
    add(tabbedPane, BorderLayout.CENTER);
  }
  
   public void addTabView(String title, JComponent jpanel) {
    int index = tabbedPane.getTabCount();
    tabbedPane.add(title, jpanel);
    ClosableTabButton ctBtn = new ClosableTabButton(tabbedPane);
    tabbedPane.setTabComponentAt(index, ctBtn);
    tabbedPane.setSelectedIndex(index);
  }
  
  public int getTabCount() { return tabbedPane.getTabCount() ; }
  
  public int getSelectedTab() { return tabbedPane.getSelectedIndex() ; }
  
  public Component getTabComponent(int idx) {
    JComponent comp = (JComponent)tabbedPane.getComponentAt(idx) ;
    if(comp instanceof JScrollPane) {
      return ((JScrollPane)comp).getViewport().getView() ;
    }
    return null ;
  }
  
  public Component getSelectedTabComponent() {
    return getTabComponent(getSelectedTab()) ;
  }
  
  private void save() {
    if(tabbedPane.getTabCount() == 0) return ;
    int selectTabIdx = getSelectedTab();
    JTextAreaEditor editor = (JTextAreaEditor) getTabComponent(selectTabIdx) ;
    boolean updateTitle = editor.file == null ;
    editor.save() ;
    if(updateTitle) {
      tabbedPane.setTitleAt(selectTabIdx, editor.label);
    }
  }
  
  private void run() {
    new Thread(new Runnable() {
      @Override 
      public void run() {
        JTextAreaEditor editor = (JTextAreaEditor) getSelectedTabComponent() ;
        ScriptWorkSpace ws = 
          SwingUtil.findAncestorOfType(ScriptEditor.this, ScriptWorkSpace.class) ;
        ws.getScriptShell().runScript(editor.getText());
      }
  }).start();

   
  }
  
  public void open(String file) {
    try {
      JTextAreaEditor editor = new JTextAreaEditor(file) ;
      String script = IOUtil.getFileContentAsString(file, "UTF-8");
      editor.setText(script);
      this.addTabView(editor.label, new JScrollPane(editor));
    } catch (Exception ex) {
      ScriptWorkSpace ws =
        SwingUtil.findAncestorOfType(ScriptEditor.this, ScriptWorkSpace.class);
      ws.getScriptShell().println(ex.toString());
    }
  }
  
  public class JTextAreaEditor extends JTextArea {
    private String file ;
    private String label = "New";
    
    public JTextAreaEditor(String file) {
      setFile(file) ;
    }
    
    public void setFile(String file) {
      this.file = file ;
      if(file != null) {
        label = new File(file).getName() ;
      }
    }
    
    void save() {
      if(file == null) {
        JFileChooser chooser = new JFileChooser();
        int returnVal = chooser.showSaveDialog(ScriptEditor.this);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
          setFile(chooser.getSelectedFile().getAbsolutePath()) ;
        } else {
          return ;
        }
      }
      try {
          IOUtil.save(getText(), "UTF-8", file);
        } catch (IOException ex) {
          ScriptWorkSpace ws =
            SwingUtil.findAncestorOfType(ScriptEditor.this, ScriptWorkSpace.class);
          ws.getScriptShell().println(ex.toString());
        }
    }
  }
}