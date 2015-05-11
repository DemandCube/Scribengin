package com.neverwinterdp.swing.widget;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JPanel;

abstract public class JSelectedItems<T> extends JPanel {
  private List<T> selectedItems = new ArrayList<T>() ;
  public JSelectedItems() {
    super(new FlowLayout(FlowLayout.LEFT));
  }
  
  public void addItem(final T item) {
    selectedItems.add(item) ;
    onModifySelectedItems() ;
  }
  
  public void addItems(Collection<T> items) {
    selectedItems.addAll(items) ;
    onModifySelectedItems() ;
  }
  
  void removeItem(T item) {
    selectedItems.remove(item) ;
    onRemoveItem(item) ;
    onModifySelectedItems() ;
  }
  
  public void onModifySelectedItems() {
    removeAll() ;
    for(final T item : selectedItems) {
      String label = getLabel(item) ;
      RemovableJLabel jlabel = new RemovableJLabel(label) {
        protected void onRemove() { removeItem(item); }
      };
      add(jlabel) ;
    }
    JButton selBtn = new JButton("..") ;
    selBtn.setPreferredSize(new Dimension(20,20));
    selBtn.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        onClickSelectButton() ;
      }
    });
    add(selBtn);
    revalidate() ;
  }
  
  abstract protected String getLabel(T item) ;
  
  protected String getTooltip(T item) { return getLabel(item) ; }
  
  abstract protected void onClickSelectButton() ;
  
  abstract protected void onRemoveItem(T item) ;
}