package com.neverwinterdp.yara.quantile;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.neverwinterdp.util.text.TabularFormater;

public class NumericDistribution {
  private Map<Long, Bucket> buckets = new HashMap<Long, Bucket>() ;
  private int total = 0 ; 
  
  public void add(int ... values) {
    for(int i = 0; i < values.length; i++) {
      add(values[i]) ;
    }
  }
  
  public void add(long ... values) {
    for(int i = 0; i < values.length; i++) {
      add(values[i]) ;
    }
  }
  
  public void add(double ... values) {
    for(int i = 0; i < values.length; i++) {
      add(values[i]) ;
    }
  }
  
  public void add(long value) {
    Bucket bucket = buckets.get(value) ;
    if(bucket == null) {
      bucket = new Bucket(value) ;
      buckets.put(value, bucket) ;
    }
    bucket.incr(); 
    total++ ;
  }
  
  
  public long[] generate(long from, long to, int size) {
    long[] set = new long[size] ;
    Random rand = new Random() ;
    long delta = to - from ;
    for(int i = 0; i < set.length; i++) {
      double randomVal = rand.nextDouble() ;
      long value = from + Math.round(randomVal * delta) ;
      set[i] = value ;
      add(value) ;
    }
    return set ;
  }
  
  public long[] getNumericSet() {
    List<Long> holder = new ArrayList<Long>() ;
    Iterator<Bucket> itr = buckets.values().iterator() ;
    while(itr.hasNext()) {
      Bucket bucket = itr.next() ;
      for(int i = 0; i < bucket.count; i++) {
        holder.add(bucket.getValue()) ;
      }
    }
    
    long[] set = new long[holder.size()] ;
    for(int i = 0; i < set.length; i++) {
      set[i] = holder.get(i) ;
    }
    return set ;
  }
  
  public void dumpBuckets() {
    Bucket[] array = getSortBuckets();
    DecimalFormat pFormater = new DecimalFormat("#.00");
    TabularFormater  tformater = new TabularFormater("#", "Value", "Frequency", "Percentile", "Acc Percentile") ;
    for(int i = 0; i < array.length; i++) {
      Bucket sel = array[i] ;
      String percentile = pFormater.format(sel.getPercentile() * 100) + "%" ;
      String accPercentile = pFormater.format(sel.accPercentile * 100) + "%" ;
      tformater.addRow(i + 1, sel.getValue(), sel.getCount(), percentile, accPercentile);
    }
    System.out.println(tformater.getFormatText());
  }
  
  public double getValueByPercentile(double percent) {
    if(percent < 0 || percent > 1) {
      throw new RuntimeException("Expect the request % between 0 - 1") ;
    }
    Bucket[] array = getSortBuckets();
    double accPercentile = 0;
    for(int i = 0; i < array.length; i++) {
      Bucket bucket = array[i] ;
      accPercentile += bucket.getPercentile() ;
      if(accPercentile >= percent) return bucket.value ;
    }
    return array[array.length - 1].value ;
  }
  
  public Bucket[] getSortBuckets() {
    Bucket[] array = new Bucket[buckets.size()];
    buckets.values().toArray(array) ;
    Arrays.sort(array, new BucketComparator());
    for(int i = 0; i < array.length; i++) {
      array[i].calPercentile(total);
    }
    double accPercentile = 0 ;
    for(int i = 0; i < array.length; i++) {
      accPercentile += array[i].getPercentile() ;
      array[i].updateAccPercentile(accPercentile);
    }
    return array ;
  }
  
  static public class Bucket {
    private long   value ;
    private int    count ;
    private double percentile ;
    private double accPercentile ;
    
    public Bucket(long value) {
      this.value = value ;
    } 
    
    public void incr() { count++ ; }
    
    public void calPercentile(int total) {
      percentile = (double)count/total ;
    }
    
    public void updateAccPercentile(double percentile) {
      this.accPercentile  = percentile ;
    }
    
    public long getValue() { return this.value ; }
    
    public int getCount() { return this.count ; }
  
    public double getPercentile() { return this.percentile ; }
    
    public double getAccPercentile() { return this.accPercentile ; }
  }
  
  static public class BucketComparator implements Comparator<Bucket> {
    public int compare(Bucket o1, Bucket o2) {
      if(o1.value < o2.value) return -1 ;
      else if(o1.value > o2.value) return 1 ;
      return 0;
    }
    
  }
}