package com.neverwinterdp.registry;

import java.util.List;

public interface MultiDataGet<T> {
  public void get(String path) ;
  
  public void get(String ... path) ;
  
  public void get(List<String> paths) ;
  
  public void getChildren(String path) throws RegistryException ;
  
  public void getChildren(Node node) throws RegistryException ;
  
  public int  getProcessErrorGetCount() ;
  
  public int  getProcessResultCount();
  
  public void shutdown() ;
  
  public List<T>          getResults() ;
  
  public List<DataGet<T>> getDataGetResults() ;
  
  public void waitForAllGet(long timeout) throws RegistryException ;
  
  static public class DataGet<T> {
    private String    path;
    private T         data;
    private ErrorCode errorCode;

    public DataGet() {} 
    
    public DataGet(String path, T data) {
      this.path = path ;
      this.data = data ;
    }
    
    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
    
    public T getData() { return data; }
    public void setData(T data) { this.data = data; }

    public ErrorCode getErrorCode() { return errorCode; }
    public void setErrorCode(ErrorCode errorCode) { this.errorCode = errorCode; }
  }
}
