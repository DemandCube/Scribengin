package com.neverwinterdp.swing.util;

import java.util.Formatter;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class ResourceManager {
  private static ResourceManager instance ;
  
  private ResourceBundle res ;
  private Formatter formatter = new Formatter() ;
  
  public ResourceManager() {
    res = ResourceBundle.getBundle("i18n.locale", Locale.getDefault());
  }
  
  public String get(String key) {
    try {
    return res.getString(key) ;
    } catch(MissingResourceException ex) {
      return key ;
    }
  }
  
  public String get(String key, Object ... params) { 
    String value = get(key) ;
    if(value == null) return key ; 
    return formatter.format(value, params).toString() ;
  }
  
  static public boolean isKeyExpression(String string) {
    return string.startsWith("${") && string.endsWith("}") ;
  }

  static public String extractKey(String string) {
    return string.substring(2, string.length() - 1) ;
  }
  
  static public ResourceManager getInstance() {
    if(instance == null) instance = new ResourceManager() ;
    return instance ;
  }
}
