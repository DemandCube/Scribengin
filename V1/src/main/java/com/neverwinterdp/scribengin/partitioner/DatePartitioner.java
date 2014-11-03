package com.neverwinterdp.scribengin.partitioner;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;



public class DatePartitioner extends AbstractPartitioner {
  SimpleDateFormat f;
  String frmtString;
  
  /**
   * Format is taken from SimpleDateFormat class:
   * G  Era designator  Text  AD
   * y Year  Year  1996; 96
   * M Month in year Month July; Jul; 07
   * w Week in year  Number  27
   * W Week in month Number  2
   * D Day in year Number  189
   * d Day in month  Number  10
   * F Day of week in month  Number  2
   * E Day in week Text  Tuesday; Tue
   * a Am/pm marker  Text  PM
   * H Hour in day (0-23)  Number  0
   * k Hour in day (1-24)  Number  24
   * K Hour in am/pm (0-11)  Number  0
   * h Hour in am/pm (1-12)  Number  12
   * m Minute in hour  Number  30
   * s Second in minute  Number  55
   * S Millisecond Number  978
   * z Time zone General time zone Pacific Standard Time; PST; GMT-08:00
   * Z Time zone RFC 822 time zone -0800
   * 
   * examples: 
   * DatePartitioner d = DatePartitioner("yy/MM_dd");
   * //returns "14/10_04" 
   * d.getPartition();
   * 
   * DatePartitioner d = DatePartitioner("yyyy/MM/dd");
   * //returns "2014/10/04" 
   * d.getPartition();
   * 
   * @author Richard Duarte
   *
   */
  public DatePartitioner(String format){
    frmtString = format;
    f = new SimpleDateFormat(format);
  }
  
  /**
   * Returns string of current time and date using the 
   * format passed in from the constructor
   */
  @Override
  public String getPartition() {
    return f.format(new Date());
  }

  /**
   * Returns the next time to refresh the partition
   */
  @Override
  public Date getRefresh() {
    Calendar cal = Calendar.getInstance();
    
    //I really hope nobody wants to change 
    //partitions every millisecond...
    if(frmtString.contains("S")){
      cal.add(Calendar.MILLISECOND, 1);
    }
    //TODO: Need to zero out everything after the second, etc
    else if(frmtString.contains("s")){
      cal.add(Calendar.SECOND, 1);
      cal.set(Calendar.MILLISECOND, 0);
    }
    else if(frmtString.contains("m")){
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.SECOND, 0);
      cal.add(Calendar.MINUTE, 1);
    }
    else if(frmtString.contains("h") || frmtString.contains("H") || 
        frmtString.contains("k") || frmtString.contains("K")){
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.add(Calendar.HOUR_OF_DAY, 1);
    }
    else if(frmtString.contains("D") || frmtString.contains("d") || 
        frmtString.contains("F") || frmtString.contains("E") ){
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.add(Calendar.DAY_OF_MONTH, 1);
    }
    else if(frmtString.contains("w") || frmtString.contains("W") ){
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.DAY_OF_MONTH, 1);
      cal.add(Calendar.WEEK_OF_MONTH, 1);
    }
    else if(frmtString.contains("M")){
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.DAY_OF_MONTH, 1);
      cal.add(Calendar.MONTH, 1);
    }
    else if(frmtString.contains("y")){
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.DAY_OF_MONTH, 1);
      cal.set(Calendar.MONTH, 0);
      cal.add(Calendar.YEAR, 1);
    }
    
    return cal.getTime();
  }

}
