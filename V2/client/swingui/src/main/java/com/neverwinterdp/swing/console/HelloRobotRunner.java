package com.neverwinterdp.swing.console;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.AbstractAction;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.SpringLayout;

import com.neverwinterdp.swing.util.SpringUtilities;

public class HelloRobotRunner extends JFrame {
  public HelloRobotRunner() {
    super("Hello Script Runner");
    addWindowListener(new WindowAdapter() {
      @Override
      public void windowClosing(WindowEvent e) {
        System.exit(0);
      }
    });
    getContentPane().setLayout(new BorderLayout());
    
    JToolBar toolbar = new JToolBar();
    toolbar.setName("ToolBar");
    toolbar.add(new AbstractAction("Test Runner!") {
      @Override
      public void actionPerformed(ActionEvent e) {
        try {
          RobotRunnerConsole scriptRunner = new RobotRunnerConsole(HelloRobotRunner.this, false);
        } catch (Exception e1) {
          e1.printStackTrace();
        }
      }
    });
    getContentPane().add(toolbar, BorderLayout.NORTH);
    
    JPanel formPanel = new InputForm() ;
    formPanel.setName("InputForm") ;
    //Lay out the panel.
    SpringUtilities.makeCompactGrid(formPanel,
                                    formPanel.getComponentCount()/2, 2, //rows, cols
                                    6, 6, //initX, initY
                                    6, 6);//xPad, yPad
    getContentPane().add(formPanel, BorderLayout.CENTER);
    
    pack();
    setVisible(true);
  }
  
  static public class InputForm extends JPanel {
    public InputForm() {
      super(new SpringLayout()) ;
      
      add("Text Field", "textField", new JTextField());
      JTextArea textArea = new JTextArea() ;
      textArea.setRows(3);
      add("Text Area", "textArea", textArea);
      
      String[] petStrings = {"Bird", "Cat", "Dog", "Rabbit", "Pig"};
      add("ComboBox", "comboBox", new JComboBox(petStrings));
      
      add("Checkbox", "checkbox", new JCheckBox());
      
      JRadioButton radioButton1 = new JRadioButton("Radio Button 1");
      JRadioButton radioButton2 = new JRadioButton("Radio Button 2");
      ButtonGroup radioButtonGroup = new ButtonGroup();
      radioButtonGroup.add(radioButton1);
      radioButtonGroup.add(radioButton2);
      add("Radio Button 1", "radioButton1", radioButton1);
      add("Radio Button 2", "radioButton2", radioButton2);
      
      JButton button = new JButton("JButton");
      button.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
          int dialogButton = JOptionPane.YES_NO_OPTION;
          int select = JOptionPane.showConfirmDialog(null, "A message", "Info", dialogButton);
        }
      });
      add("Button", "JButton", button);
    }
    
    void add(String label, String name, JComponent component) {
      JLabel l = new JLabel(label, JLabel.TRAILING);
      add(l);
      component.setName(name);
      l.setLabelFor(component);
      add(component);
    }
  }
  
  static public void main(String[] args) throws Exception {
    HelloRobotRunner tester = new HelloRobotRunner();
    Thread.currentThread().join();
  }
}