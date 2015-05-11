package com.neverwinterdp.swing.widget;

import java.awt.Component;
import java.awt.GridLayout;

import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

@SuppressWarnings("serial")
public class GridLayoutPanel extends JPanel {
  private GridLayout layout ;
  public GridLayoutPanel(int row, int col) {
    layout = new GridLayout(row, col) ;
    setLayout(layout) ;
  }
  
  public GridLayoutPanel() {
    layout = new GridLayout() ;
    setLayout(layout) ;
  }
  
  public void addCell(Object comp) {
    if(comp instanceof String) {
      add(new JLabel((String) comp)) ;
    } else if(comp instanceof Action) {
      add(new JButton((Action) comp)) ;
    } else if(comp instanceof Component) {
      add((Component) comp) ;
    } else {
      throw new RuntimeException("Not support type " + comp.getClass()) ;
    }
  }
  
  public void addCells(Object ... comp) {
    for(Object sel : comp) addCell(sel);
  }
  
  public void makeGrid(int rows, int cols) {
    layout.setRows(rows) ;
    layout.setColumns(cols) ;
  }
  
  public void makeGrid(int cols) {
    int rows = (int)Math.ceil(getComponentCount() /(double)cols) ;
    makeGrid(rows, cols) ;
  }
}