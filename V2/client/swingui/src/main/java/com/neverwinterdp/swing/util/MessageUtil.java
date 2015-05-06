package com.neverwinterdp.swing.util;

import javax.swing.JOptionPane;

import com.neverwinterdp.util.ExceptionUtil;

public class MessageUtil {
  static public void error(String message) {
    JOptionPane.showMessageDialog(null, message);
  }
  
  static public void handleError(Throwable t) {
    String stacktrace = ExceptionUtil.getStackTrace(t) ;
    System.err.println(stacktrace);
    JOptionPane.showMessageDialog(null, stacktrace);
  }
}