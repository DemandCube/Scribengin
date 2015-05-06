package com.neverwinterdp.swing.widget;

import java.awt.Component;

import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SpringLayout;

import com.neverwinterdp.swing.util.SpringUtilities;

public class SpringLayoutGridJPanel extends JPanel {
  private int numberOfRow = 0;
  private int numberOfColumn = -1 ;
  
  public SpringLayoutGridJPanel() {
    setLayout(new SpringLayout()) ;
  }
  
  public void addRow(Object ... comp) {
    if(numberOfColumn < 0) numberOfColumn = comp.length ;
    if(numberOfColumn != comp.length) {
      throw new RuntimeException("Expect " + numberOfColumn + " columns") ;
    }
    for(Object sel : comp) addCell(sel) ;
    numberOfRow++ ;
  }
  
  public void addCell(Object comp) {
    if(comp instanceof String || comp instanceof Integer || comp instanceof Long || comp instanceof Boolean) {
      add(new JLabel(comp.toString())) ;
    } else if(comp instanceof Action) {
      Action action = (Action) comp ;
      JButton button = new JButton() ;
      button.setAction(action) ;
      add(button) ;
    } else if(comp instanceof Component) {
      add((Component) comp) ;
    } else {
      throw new RuntimeException("Not support type " + comp.getClass()) ;
    }
  }
  
  public void makeCompactGrid() {
    if(numberOfColumn < 0) {
      throw new RuntimeException("No components are added") ;
    }
    SpringUtilities.makeCompactGrid(this, /*beans, cols*/numberOfRow, numberOfColumn,  /*initX, initY*/ 3, 3, /*xPad, yPad*/3, 3);
  }
  
  public void makeCompactGrid(int numberOfColumn) {
    this.numberOfColumn = numberOfColumn ;
    this.numberOfRow = (int)Math.ceil(getComponentCount()/(double)numberOfColumn) ;
    SpringUtilities.makeCompactGrid(this, /*beans, cols*/numberOfRow, numberOfColumn,  /*initX, initY*/ 3, 3, /*xPad, yPad*/3, 3);
  }
  
  public void makeGrid() {
    if(numberOfColumn < 0) {
      throw new RuntimeException("No components are added") ;
    }
    SpringUtilities.makeGrid(this, /*beans, cols*/numberOfRow, numberOfColumn,  /*initX, initY*/ 3, 3, /*xPad, yPad*/3, 3);
  }
  
  public void makeGrid(int initX, int initY, int xPad, int yPad) {
    if(numberOfColumn < 0) {
      throw new RuntimeException("No components are added") ;
    }
    SpringUtilities.makeCompactGrid(this, /*beans, cols*/numberOfRow, numberOfColumn,  initX, initY, xPad, yPad);
  }
  
  public void createBorder(String title) {
    setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), title));
  }
  
  public void createBorder() {
    setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder()));
  }
}
