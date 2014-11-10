package com.neverwinterdp.scribengin.registry;

public class RegistryException extends Exception {
  private static final long serialVersionUID = 1L;
  
  private ErrorCode errorCode = ErrorCode.Unknown ;
  
  public RegistryException(ErrorCode code, String message) {
    super(message) ;
    this.errorCode = code ;
  }
  
  public RegistryException(ErrorCode code, Throwable cause) {
    super(cause) ;
    this.errorCode = code;
  }
  
  public RegistryException(ErrorCode code, String message, Throwable cause) {
    super(message, cause) ;
    this.errorCode = code ;
  }
  
  public ErrorCode getErrorCode() { return this.errorCode; }
}
