package com.neverwinterdp.vm.command;

public class CommandResult<T> {
  private T result ;

  public <V> V getResultAs(Class<V> type) { return (V) result; }
  
  public T getResult() { return result; }
  public void setResult(T result) { this.result = result; }
}
