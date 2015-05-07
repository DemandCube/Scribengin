package com.neverwinterdp.swing.widget;

import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.JCheckBox;

import com.neverwinterdp.swing.util.BeanInspector;

public class BeanBindingJCheckBox<T> extends JCheckBox {
  private T bean ;
  private String beanProperty ;
  private BeanInspector<T> beanInspector ;
  
  public BeanBindingJCheckBox(T aBean, String property) {
    setName(property) ;
    bean = aBean ;
    beanProperty = property ;
    beanInspector = BeanInspector.get(bean.getClass()) ;
    setSelected(beanInspector.getValue(bean, property)) ;
    addItemListener(new ItemListener() {
      public void itemStateChanged(ItemEvent e) {
        beanInspector.setValue(bean, beanProperty,  e.getStateChange() == ItemEvent.SELECTED) ;
      }
    });
  }
  
  public void setSelected(Object value) {
    if(value == null) setSelected(false) ;
    else if(value instanceof Boolean) super.setSelected((Boolean)value) ;
    else throw new RuntimeException("Expect Boolean type instead of " + value.getClass()) ;
  }
  
  public void setBean(T bean) {
    this.bean = bean ;
    setSelected(beanInspector.getValue(bean, beanProperty)) ;
  }
}
