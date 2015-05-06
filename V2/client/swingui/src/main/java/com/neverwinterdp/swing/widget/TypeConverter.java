package com.neverwinterdp.swing.widget;

import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;

import com.neverwinterdp.swing.util.text.DateUtil;
import com.neverwinterdp.swing.util.text.StringUtil;

public interface TypeConverter {
  public Object convert(String text) ;
  public Format getFormat() ;

  static abstract public class DoubleConverter implements TypeConverter {
    public Object convert(String text) {
      if(StringUtil.isEmpty(text)) return new Double(0) ;
      return Double.parseDouble(text);
    }
  }

  static public TypeConverter CURRENCY = new DoubleConverter() {
    private NumberFormat format ;
    
    public Format getFormat() {
      if(format == null) {
        format = NumberFormat.getNumberInstance() ;
        format.setMinimumFractionDigits(2) ;
        format.setMaximumFractionDigits(2) ;
      }
      return format ;
    }
  };
  
  static public TypeConverter PERCENT = new DoubleConverter() {
    private NumberFormat format ;
    
    public Format getFormat() {
      if(format == null) {
        format = NumberFormat.getNumberInstance() ;
        format.setMaximumFractionDigits(3) ;
      }
      return format ;
    }
  };

  static public TypeConverter DATE = new  TypeConverter() {
    public Object convert(String text) {
      if(StringUtil.isEmpty(text)) return null ;
      try {
        return DateUtil.COMPACT_DATE.parse(text);
      } catch (ParseException e) {
        throw new RuntimeException(e) ;
      }
    }
    
    public Format getFormat() { return DateUtil.COMPACT_DATE ;}
  };
  
  static public TypeConverter DATE_TIME = new  TypeConverter() {
    public Object convert(String text) {
      if(StringUtil.isEmpty(text)) return null ;
      try {
        return DateUtil.COMPACT_DATE_TIME.parse(text);
      } catch (ParseException e) {
        throw new RuntimeException(e) ;
      }
    }
    
    public Format getFormat() { return DateUtil.COMPACT_DATE_TIME ;}
  };
  
  static public TypeConverter INTERGER = new  TypeConverter() {
    public Object convert(String text) {
      if(text == null) return null ;
      return Integer.parseInt(text);
    }
    
    public Format getFormat() {
      return null ;
    }
  };
}