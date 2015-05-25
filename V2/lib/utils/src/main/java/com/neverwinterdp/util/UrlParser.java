package com.neverwinterdp.util;

public class UrlParser {
  private String scheme = "http";
  private String host ;
  private int port ;
  private String path ;
  
  public UrlParser(String addressUrl) {
    if(addressUrl.startsWith("http://")) {
      addressUrl = addressUrl.substring("http://".length()) ;
      scheme = "http" ;
    }
    
    if(addressUrl.startsWith("https://")) {
      addressUrl = addressUrl.substring("https://".length()) ;
      scheme = "https" ;
    }
    
    int idx = addressUrl.indexOf("/") ;
    String hostport = addressUrl ;
    if(idx > 0) {
      hostport = addressUrl.substring(0, idx) ;
      path = addressUrl.substring(idx) ;
    } 
    int colonIdx = hostport.indexOf(":") ;
    if(colonIdx > 0) {
      host = hostport.substring(0, colonIdx) ;
      port = Integer.parseInt(hostport.substring(colonIdx + 1)) ;
    }
  }

  public String getScheme() { return scheme; }

  public String getHost() { return host; }

  public int getPort() { return port; }

  public String getPath() { return path; }
}
