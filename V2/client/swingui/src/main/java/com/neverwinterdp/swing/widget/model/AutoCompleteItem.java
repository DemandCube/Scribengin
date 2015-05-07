package com.neverwinterdp.swing.widget.model;

public class AutoCompleteItem<T> {
  private String display ;
  private T      value ;
  
  public AutoCompleteItem() {}
  
  public AutoCompleteItem(String display, T value) {
    this.display = display ;
    this.value = value ;
  }
  
  public String getDisplay() { return display; }
  public void setDisplay(String display) { this.display = display; }
  
  public T getValue() { return value; }
  public void setValue(T value) { this.value = value; }
  
  public String toString() { return display ; }
}
