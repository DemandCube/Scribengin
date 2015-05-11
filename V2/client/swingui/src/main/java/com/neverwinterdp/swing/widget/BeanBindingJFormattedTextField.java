package com.neverwinterdp.swing.widget;

import java.text.NumberFormat;

import javax.swing.JFormattedTextField;

import com.neverwinterdp.swing.util.BeanInspector;
import com.neverwinterdp.swing.widget.listener.JTextFieldChangeTextListener;

public class BeanBindingJFormattedTextField<T> extends JFormattedTextField implements BeanBinding {
  final static public NumberFormat CURRENCY, PERCENT ;
  
  static {
    CURRENCY = NumberFormat.getNumberInstance() ;
    CURRENCY.setMinimumFractionDigits(2) ;
    CURRENCY.setMaximumFractionDigits(2) ;
    
    PERCENT = NumberFormat.getNumberInstance() ;
    PERCENT.setMinimumFractionDigits(3) ;
  }
  
  private T bean ;
  private String beanProperty ;
  private BeanInspector<T> beanInspector ;
  private TypeConverter typeConverter ;
  private boolean onBeanPropertyChange = false ;
  
  public BeanBindingJFormattedTextField(T aBean, String property, TypeConverter converter) {
    this(aBean, property, converter, true) ;
  }
  
  public BeanBindingJFormattedTextField(T aBean, String property, TypeConverter converter, boolean editable) {
    super(converter != null? converter.getFormat() : null) ;
    setName(property) ;
    setEditable(editable) ;
    this.typeConverter = converter ;
    beanProperty = property ;
    beanInspector = BeanInspector.get(aBean.getClass()) ;
    setBean(aBean) ;
    getDocument().addDocumentListener(new JTextFieldChangeTextListener() {
      public void onChange(String text) { 
        if(onBeanPropertyChange) return ;
        text = onTextChange(text) ;
        if(typeConverter == null) {
          beanInspector.setValue(bean, beanProperty, text) ;
        } else {
          try { 
            beanInspector.setValue(bean, beanProperty, typeConverter.convert(text)) ;
          } catch(Throwable t) {
            System.err.println("property = " + beanProperty + ", text = " + text);
            t.printStackTrace();
          }
        }
      }
    });
  }
  
  public void setText(Object value) {
    if(value == null) {
      setText("") ;
    } else {
      if(typeConverter != null) setText(typeConverter.getFormat().format(value));
      else setText(value.toString()) ;
    }
  }
  
  public void setBean(T bean) {
    this.bean = bean ;
    setText(beanInspector.getValue(bean, beanProperty)) ;
  }
  
  public void setBeanValue(Object val) {
    setText(val) ;
  }
  
  public String onTextChange(String text) {
    return text; 
  }

  public void onBeanPropertyChange() {
    onBeanPropertyChange = true ;
    Object val = beanInspector.getValue(bean, beanProperty) ;
    setText(val) ;
    onBeanPropertyChange = false ;
  }
}
