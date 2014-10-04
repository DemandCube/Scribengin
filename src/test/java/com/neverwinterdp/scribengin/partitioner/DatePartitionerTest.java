package com.neverwinterdp.scribengin.partitioner;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;

import org.junit.Test;


public class DatePartitionerTest {
  @Test
  public void testDatePartitionerGetPartition(){
    DatePartitioner d = new DatePartitioner("yy/MM/dd");
    
    String year = Integer.toString(Calendar.getInstance().get(Calendar.YEAR)).substring(2);
    
    //For whatever reason, months in the Calendar class start at @#$%ing zero
    String month = Integer.toString(Calendar.getInstance().get(Calendar.MONTH)+1);
    
    String day = Integer.toString(Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
    if(day.length() == 1){
      day = "0"+day;
    }
    
    assertEquals(year+"/"+month+"/"+day,d.getPartition());
  }
  
  
  @Test
  public void testDatePartitionerGetPartition2(){
    DatePartitioner d = new DatePartitioner("yyyy/MM/dd/HH/mm/ss");
    
    String year = Integer.toString(Calendar.getInstance().get(Calendar.YEAR));
    
    //For whatever reason, months in the Calendar class start at @#$%ing zero
    String month = Integer.toString(Calendar.getInstance().get(Calendar.MONTH)+1);
    
    String day = Integer.toString(Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
    if(day.length() == 1){
      day = "0"+day;
    }
    
    String hour = Integer.toString(Calendar.getInstance().get(Calendar.HOUR_OF_DAY));
    if(hour.length() == 1){
      hour = "0"+hour;
    }
    
    String min = Integer.toString(Calendar.getInstance().get(Calendar.MINUTE));
    if(min.length() == 1){
      min = "0"+min;
    }
    
    //Hopefully this gets completed within a second for the assertion to work
    String sec = Integer.toString(Calendar.getInstance().get(Calendar.SECOND));
    if(sec.length() == 1){
      sec = "0"+sec;
    }
    
    assertEquals(year+"/"+month+"/"+day+"/"+hour+"/"+min+"/"+sec,d.getPartition());
  }
  
  @Test
  public void testDatePartitionerGetRefreshYear(){
    DatePartitioner d = new DatePartitioner("yyyy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.YEAR, 1);
    
    assertEquals(year.getTime(),d.getRefresh());
  }
  
  @Test
  public void testDatePartitionerGetRefreshMonth(){
    DatePartitioner d = new DatePartitioner("MM/yyyy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.MONTH, 1);
    
    assertEquals(year.getTime(),d.getRefresh());
  }
  
  @Test
  public void testDatePartitionerGetRefreshDay(){
    DatePartitioner d = new DatePartitioner("dd/MM/yy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.DAY_OF_YEAR, 1);
    
    assertEquals(year.getTime(),d.getRefresh());
  }
  
  @Test
  public void testDatePartitionerGetRefreshHour(){
    DatePartitioner d = new DatePartitioner("HH/dd/MM/yy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.HOUR_OF_DAY, 1);
    
    assertEquals(year.getTime(),d.getRefresh());
  }
  
  @Test
  public void testDatePartitionerGetRefreshMinute(){
    DatePartitioner d = new DatePartitioner("mm/HH/dd/MM/yy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.MINUTE, 1);
    
    assertEquals(year.getTime(),d.getRefresh());
  }
  
  @Test
  public void testDatePartitionerGetRefreshSecond(){
    DatePartitioner d = new DatePartitioner("ss/mm/HH/dd/MM/yy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.SECOND, 1);
    
    assertEquals(year.getTime(),d.getRefresh());
  }
  
  @Test
  public void testDatePartitionerGetRefreshMilliSecond(){
    DatePartitioner d = new DatePartitioner("SS/ss/mm/HH/dd/MM/yy");
    Calendar year = Calendar.getInstance();
    year.add(Calendar.MILLISECOND, 1);
    
    //assertEquals(year.getTime(),d.getRefresh());
  }
}
