package com.neverwinterdp.swing.widget;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JComboBox;

import com.neverwinterdp.swing.util.BeanInspector;

public class BeanBindingJComboBox<B, T> extends JComboBox<T> {
  private B bean ;
  private String beanProperty ;
  private BeanInspector<B> beanInspector ;
  
  public BeanBindingJComboBox(B aBean, String property, T[] option) {
    super(option) ;
    setName(property) ;
    bean = aBean ;
    beanProperty = property ;
    beanInspector = BeanInspector.get(bean.getClass()) ;
    setSelectedItem(beanInspector.getValue(bean, property)) ;
    addActionListener (new ActionListener () {
      public void actionPerformed(ActionEvent e) {
        Object selectItem = getSelectedItem() ;
        beanInspector.setValue(bean, beanProperty, selectItem) ; 
        setSelectedItem(selectItem) ;
      }
    });
  }
  
  public void setSelectedItem(Object value) {
    super.setSelectedItem((T)value) ;
  }
  
  public void updateBeanValue(Object value) {
    beanInspector.setValue(bean, beanProperty, value) ;
    setSelectedItem(value) ;
  }
}
