package com.neverwinterdp.jvmagent.bootstrap;

import java.io.File;
import java.io.FileInputStream;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PremainBootstrap {
  public static void premain(String pluginPath, Instrumentation inst) throws Exception {
    ClassLoader oldClassloader = Thread.currentThread().getContextClassLoader() ;
    try {
      Properties props = new Properties() ;
      props.load(new FileInputStream(pluginPath + "/agent.properties"));
      String pluginClassName = props.getProperty("plugin.class") ;
      boolean newClassloader = "true".equalsIgnoreCase(props.getProperty("plugin.classloader.isolate"));

      ClassLoader cl = null ;
      if(newClassloader) {
        File pluginDir = new File(pluginPath);
        List<URL> jars = new ArrayList<URL>();
        for(File sel : pluginDir.listFiles()) {
          String name = sel.getName();
          if(name.endsWith(".jar")) {
            URL url = new URL("file:" + sel.getCanonicalPath());
            jars.add(url);
            System.out.println("add jar: " + url);
          }
        }
        URL[] jarPaths = new URL[jars.size()];
        jarPaths = jars.toArray(jarPaths);
        cl = new URLClassLoader(jarPaths, Thread.currentThread().getContextClassLoader()) ;
      } else {
        cl = Thread.currentThread().getContextClassLoader();
      }
      Thread.currentThread().setContextClassLoader(cl);

      Class<BootstrapAgentPlugin> pluginClass = (Class<BootstrapAgentPlugin>) cl.loadClass(pluginClassName);
      BootstrapAgentPlugin plugin = pluginClass.newInstance();

      props.remove("plugin.class");
      props.remove("plugin.classloader.isolate");

      plugin.run(props, inst);
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassloader);
    }
  }
}