package com.neverwinterdp.swing.widget;

import javax.swing.JLabel;

import com.neverwinterdp.swing.util.BeanInspector;

public class BeanBindingJLabel<T> extends JLabel implements BeanBinding {
  private T bean ;
  private String beanProperty ;
  private BeanInspector<T> beanInspector ;
  
  public BeanBindingJLabel(T aBean, String property) {
    setName(property) ;
    beanProperty = property ;
    beanInspector = BeanInspector.get(aBean.getClass()) ;
    setBean(aBean) ;
  }
  
  public void setText(Object value) {
    if(value == null) setText("") ;
    else setText(value.toString()) ;
  }
  
  public void setBean(T bean) {
    this.bean = bean ;
    setText(beanInspector.getValue(bean, beanProperty)) ;
  }
  
  public void onBeanPropertyChange() {
    setText(beanInspector.getValue(bean, beanProperty)) ;
  }
}