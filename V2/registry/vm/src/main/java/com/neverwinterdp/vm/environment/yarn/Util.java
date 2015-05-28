package com.neverwinterdp.vm.environment.yarn;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

public class Util {
  static public void setupAppMasterEnv(boolean miniClusterEnv, Configuration conf, Map<String, String> appMasterEnv) {
    if(miniClusterEnv) {
      String cps = System.getProperty("java.class.path") ;
      String[] cp = cps.split(":") ;
      for(String selCp : cp) {
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), selCp, ":");
      }
    } else {
      StringBuilder classPathEnv = new StringBuilder();
      classPathEnv.append(Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
      classPathEnv.append("./*");

      String[] classpath = conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH
      ) ;
      for (String selClasspath : classpath) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(selClasspath.trim());
        System.out.println("add classpath: " + selClasspath);
      }

      String envStr = classPathEnv.toString();
      appMasterEnv.put(Environment.CLASSPATH.name(), envStr);
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*", ":");
  }
}
