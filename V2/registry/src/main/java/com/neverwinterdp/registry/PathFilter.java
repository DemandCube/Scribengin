package com.neverwinterdp.registry;

import java.util.regex.Pattern;

public interface PathFilter {
  public boolean accept(String path) ;
  
  static public class IgnorePathFilter implements PathFilter {
    private Pattern pattern ;
    
    public IgnorePathFilter(String regex) {
      pattern = Pattern.compile(regex);
    }
    
    @Override
    public boolean accept(String path) {
      if(pattern.matcher(path).matches()) {
        System.err.println("ignore path = " + path);
        return false;
      }
      System.err.println("accept path = " + path);
      return true;
    }
    
  }
}
