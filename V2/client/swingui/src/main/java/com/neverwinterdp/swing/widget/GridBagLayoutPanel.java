package com.neverwinterdp.swing.widget;


import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.neverwinterdp.swing.util.ResourceManager;

public class GridBagLayoutPanel extends JPanel {
  private GridBagConstraints constraint;

  public GridBagLayoutPanel() {
    setLayout(new GridBagLayout());
    constraint = new GridBagConstraints();
    constraint.fill = GridBagConstraints.HORIZONTAL;
    constraint.insets = new Insets(2, 2, 2, 2);
  }

  public GridBagLayoutPanel(GridBagConstraints constraint) {
    setLayout(new GridBagLayout());
    this.constraint = constraint;
  }
  
  public void setInsets(Insets insets){
    constraint.insets= insets;
  }

  public void add(int row, int col, Component comp) {
    constraint.gridx = col;
    constraint.gridy = row;
    constraint.weightx = 1;
    constraint.gridwidth = 1;
    add(comp, constraint);
  }

  public void add(int row, int col, String label) {
    constraint.gridx = col;
    constraint.gridy = row;
    constraint.weightx = 0;
    constraint.gridwidth = 1;
    if (ResourceManager.isKeyExpression(label)) {
      String key = ResourceManager.extractKey(label);
      add(new LocalizableJLabel(ResourceManager.getInstance(), key), constraint);
    } else {
      add(new JLabel(label), constraint);
    }
  }

  public void add(int row, int col, Component comp, int colspan) {
    constraint.gridx = col;
    constraint.gridy = row;
    constraint.gridwidth = colspan;
    constraint.weightx = 1;
    add(comp, constraint);
  }

  public void createBorder() {
    setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder()));
  }

  public void createBorder(String title) {
    setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), title));
  }
}