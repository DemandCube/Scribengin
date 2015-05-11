package com.neverwinterdp.swing.widget;

import javax.swing.JTextField;

import com.neverwinterdp.swing.util.BeanInspector;
import com.neverwinterdp.swing.widget.listener.JTextFieldChangeTextListener;

public class BeanBindingJTextField<T> extends JTextField implements BeanBinding {
  private T bean ;
  private String beanProperty ;
  private BeanInspector<T> beanInspector ;
  private TypeConverter typeConverter ;
  
  public BeanBindingJTextField(T aBean, String property) {
    this(aBean, property, true) ;
  }
  
  public BeanBindingJTextField(T aBean, String property, boolean editable) {
    setName(property) ;
    setEditable(editable) ;
    beanProperty = property ;
    beanInspector = BeanInspector.get(aBean.getClass()) ;
    setBean(aBean) ;
    getDocument().addDocumentListener(new JTextFieldChangeTextListener() {
      public void onChange(String text) { 
        text = onTextChange(text) ;
        if(typeConverter == null) {
          beanInspector.setValue(bean, beanProperty, text) ;
        } else {
          beanInspector.setValue(bean, beanProperty, typeConverter.convert(text)) ;
        }
      }
    });
  }
  
  public void setTypeConverter(TypeConverter tConverter) {
    this.typeConverter = tConverter ;
  }
  
  public void setText(Object value) {
    if(value == null) setText("") ;
    else setText(value.toString()) ;
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
    Object val = beanInspector.getValue(bean, beanProperty) ;
    setText(val) ;
  }
  
  static public interface TypeConverter {
    public Object convert(String text) ;
  }
  
  static public TypeConverter DOUBLE = new  TypeConverter() {
    public Object convert(String text) {
      if(text == null) return null ;
      return Double.parseDouble(text);
    }
  };
  
  static public TypeConverter INTERGER = new  TypeConverter() {
    public Object convert(String text) {
      if(text == null) return null ;
      return Integer.parseInt(text);
    }
  };
}
