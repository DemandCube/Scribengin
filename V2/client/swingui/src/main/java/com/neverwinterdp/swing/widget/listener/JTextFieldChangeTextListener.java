package com.neverwinterdp.swing.widget.listener;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.Document;

abstract public class JTextFieldChangeTextListener implements DocumentListener {
  public void changedUpdate(DocumentEvent e) { }
  
  public void removeUpdate(DocumentEvent e) {
    Document doc = e.getDocument() ;
    try {
      String text = doc.getText(0, doc.getLength()) ;
      onChange(text) ;
    } catch(Exception ex) {
    }
  }

  public void insertUpdate(DocumentEvent e) {
    Document doc = e.getDocument() ;
    try {
      String text = doc.getText(0, doc.getLength()) ;
      onChange(text) ;
    } catch(Exception ex) {
    }
  }

  abstract public void onChange(String text) ;
}