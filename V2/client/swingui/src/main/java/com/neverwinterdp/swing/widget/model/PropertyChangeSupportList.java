package com.neverwinterdp.swing.widget.model;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;

public class PropertyChangeSupportList<T> extends ArrayList<T>{
  private PropertyChangeSupport pcs = new PropertyChangeSupport(this);

  public boolean add(T bean) {
    boolean added = super.add(bean) ;
    if(added) {
      pcs.firePropertyChange("add", bean, bean);
    }
    return added ;
  }
  
  public T remove(int idx) {
    T bean = super.remove(idx) ;
    pcs.firePropertyChange("remove", bean, bean);
    return bean ;
  }
  
  public void addPropertyChangeListener(PropertyChangeListener listener) {
    pcs.addPropertyChangeListener(listener);
  }
  
  public void addPropertyChangeListener(String propertyName, PropertyChangeListener listener) {
    pcs.addPropertyChangeListener(propertyName, listener);
  }
  
  public void removePropertyChangeListener(PropertyChangeListener listener) {
    pcs.removePropertyChangeListener(listener) ; 
  }
  
  public void removePropertyChangeListener(String propertyName, PropertyChangeListener listener) {
    pcs.removePropertyChangeListener(propertyName, listener) ; 
  }
}
