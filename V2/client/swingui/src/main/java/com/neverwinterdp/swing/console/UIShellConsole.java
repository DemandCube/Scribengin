package com.neverwinterdp.swing.console;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.SwingUtilities;

import com.neverwinterdp.swing.script.ScriptRunner;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.Fonts;

@SuppressWarnings("serial")
public class UIShellConsole extends JPanel {

  private Map<String, AbstractCommand> commands;
  private JTextPane                    textPane;
  private JTextField                   input;

  public UIShellConsole() throws Exception {
    super(new BorderLayout());
    commands = new LinkedHashMap<String, AbstractCommand>();
    commands.put("help", new HelpCommand());
    commands.put("clear", new ClearCommand());
    commands.put("uitree", new UITreeCommand());

    input = new JTextField();
    input.addKeyListener(new KeyAdapter() {
      @Override
      public void keyTyped(KeyEvent e) {
        if (KeyEvent.VK_ENTER == e.getKeyChar()) {
          String command = input.getText();
          input.setText("");
          execute(command);
        }
      }
    });
    
    textPane = new JTextPane();
    textPane.setEditable(false);
    textPane.setFont(Fonts.FIXED) ;
    JPanel noWrapPanel = new JPanel(new BorderLayout());
    noWrapPanel.add(textPane);
    add(new JScrollPane(noWrapPanel), BorderLayout.CENTER);
    add(input, BorderLayout.SOUTH);
    
    redirectSystemStreams();
  }

  public void print(String msg) {
    String text = textPane.getText();
    if (text == null) {
      textPane.setText(msg);
    } else {
      textPane.setText(text + msg);
    }
  }

  public void clear() { textPane.setText(""); }

  public void println(String[] msg, int[] width) {
    for(int i = 0; i < msg.length; i++) {
      printCell(msg[i], width[i]) ;
    }
    print("\n") ;
  }
  
  public void printCell(String msg, int width) {
    if (msg.length() > width) {
      msg = msg.substring(0, width);
    } else {
      StringBuilder b = new StringBuilder(msg);
      for (int i = msg.length(); i < width; i++) {
        b.append(' ');
      }
      msg = b.toString();
    }
    print(msg);
  }

  public void println(String msg) {
    print(msg + "\n");
  }

  public void runScript() {
    runScript(textPane.getText());
  }

  public void runScript(String script) {
    RobotRunnerConsole dialog = SwingUtil.findAncestorOfType(UIShellConsole.this, RobotRunnerConsole.class);
    ScriptRunner scriptRunner = dialog.getScriptRunner();
    try {
      Object ret = scriptRunner.eval(script);
      if (ret != null) {
        println(ret.toString());
      }
    } catch (Exception ex) {
      println(ex.getMessage());
    }
  }

  public void execute(String commandName) {
    AbstractCommand command = commands.get(commandName);
    if (command != null) {
      println("");
      println("$" + commandName);
      command.execute();
    } else {
      runScript(commandName);
    }
  }
  
  
  private void updateTextArea(final String text) {
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        print(text);
      }
    });
  }
 
  private void redirectSystemStreams() {
    OutputStream out = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        updateTextArea(String.valueOf((char) b));
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        updateTextArea(new String(b, off, len));
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }
    };

    System.setOut(new PrintStream(out, true));
    //System.setErr(new PrintStream(out, true));
  }
  
  abstract public class AbstractCommand {

    public String getDescription() {
      return "No description";
    }

    abstract public void execute();
  }

  public class HelpCommand extends AbstractCommand {

    public String getDescription() {
      return "Print the list of the available commands!";
    }

    public void execute() {
      for (Map.Entry<String, AbstractCommand> entry : commands.entrySet()) {
        printCell(entry.getKey() + ": ", 20);
        print(entry.getValue().getDescription() + "\n");
      }
    }
  }

  public class ClearCommand extends AbstractCommand {

    public String getDescription() {
      return "Clear the console!";
    }

    public void execute() {
      textPane.setText("");
    }
  }

  public class UITreeCommand extends AbstractCommand {

    public String getDescription() {
      return "Print the ui tree!";
    }

    public void execute() {
      RobotRunnerConsole dialog =
          SwingUtil.findAncestorOfType(UIShellConsole.this, RobotRunnerConsole.class);
      Component uiroot = UIShellConsole.this;
      StringBuilder b = new StringBuilder();
      printTree(b, uiroot, "");
      println(b.toString());
    }

    private void printTree(StringBuilder b, Component comp, String indent) {
      if (indent != null) {
        b.append(indent);
      }
      String name = comp.getName();
      if (name == null) {
        name = "Undefined";
      }
      b.append(name).append("(").append(comp.getClass().getSimpleName()).append(")\n");
      if (comp instanceof Container) {
        Container container = (Container) comp;
        for (int i = 0; i < container.getComponentCount(); i++) {
          Component child = container.getComponent(i);
          printTree(b, child, indent + "    ");
        }
      }
    }
  }
}