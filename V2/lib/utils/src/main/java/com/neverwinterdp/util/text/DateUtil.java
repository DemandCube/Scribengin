package com.neverwinterdp.util.text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
  final static public int SECONDS_PER_DAY = 86400;

  final static public SimpleDateFormat COMPACT_DATE = new SimpleDateFormat("dd/MM/yyyy")  ;
  final static public SimpleDateFormat COMPACT_DATE_TIME = new SimpleDateFormat("dd/MM/yyyy@HH:mm:ss")  ;
  final static public SimpleDateFormat COMPACT_DATE_ID = new SimpleDateFormat("yyyyMMdd")  ;
  final static public SimpleDateFormat COMPACT_DATE_TIME_ID = new SimpleDateFormat("yyyyMMddHHmmss") ;
  
  static public String asCompactDate(long time) {
    if(time <= 0) return "N/A";
    return COMPACT_DATE.format(new Date(time)) ;
  }
  
  static public String asCompactDate(Date time) {
    return COMPACT_DATE.format(time) ;
  }
  
  static public String asCompactDateId(Date time) {
    return COMPACT_DATE_ID.format(time) ;
  }
  
  static public String asCompactDate(Calendar cal) {
    return COMPACT_DATE.format(cal.getTime()) ;
  }
  
  static public String asCompactDateTime(long time) {
    if(time <= 0) return "N/A";
    return COMPACT_DATE_TIME.format(new Date(time)) ;
  }
  
  static public String asCompactDateTIME(Date time) {
  	return COMPACT_DATE_TIME.format(time) ;
  }
  
  static public Date parseCompactDate(String exp) throws ParseException {
    return COMPACT_DATE.parse(exp) ;
  }
  
  static public Date parseCompactDateTime(String exp) throws ParseException {
    return COMPACT_DATE_TIME.parse(exp) ;
  }
  
  static public Date parseCompactDateTimeId(String exp) throws ParseException {
    return COMPACT_DATE_TIME_ID.parse(exp) ;
  }
  
  static public String asCompactDateTimeId(long time) {
    return COMPACT_DATE_TIME_ID.format(new Date(time)) ;
  }
  
  static public String asCompactDateTimeId(Date date) {
    return COMPACT_DATE_TIME_ID.format(date) ;
  }
  
  static public String currentTimePath(String basePath) {
    String backupTime = basePath + "/" + COMPACT_DATE_TIME_ID.format(new Date());
    return backupTime ;
  }
  
  static public String currentDatePath(String basePath) {
    String backupTime = basePath + "/" + COMPACT_DATE_ID.format(new Date());
    return backupTime ;
  }
}
