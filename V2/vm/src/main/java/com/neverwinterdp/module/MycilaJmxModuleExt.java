package com.neverwinterdp.module;

import java.lang.management.ManagementFactory;

import javax.inject.Provider;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.mycila.guice.ext.injection.ClassToTypeLiteralMatcherAdapter;
import com.mycila.jmx.ExportBehavior;
import com.mycila.jmx.JmxExporter;
import com.mycila.jmx.JmxNamingStrategy;
import com.mycila.jmx.JmxSelfNaming;
import com.mycila.jmx.MycilaJmxExporter;
import com.mycila.jmx.annotation.JmxBean;

public class MycilaJmxModuleExt extends AbstractModule {
  private String domain ;
  
  public MycilaJmxModuleExt(String domain) {
    this.domain = domain ;
  }
  
  @Override
  protected void configure() {
    bindListener(ClassToTypeLiteralMatcherAdapter.adapt(Matchers.annotatedWith(JmxBean.class)), new TypeListener() {
      @Override
      public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
        final Provider<JmxExporter> exporter = encounter.getProvider(JmxExporter.class);
        encounter.register(new InjectionListener<I>() {
          @Override
          public void afterInjection(I injectee) {
            exporter.get().register(injectee);
          }
        });
      }
    });
  }

  @Provides @Singleton
  protected JmxExporter jmxExporter(MBeanServer server) {
    MycilaJmxExporter exporter = new MycilaJmxExporter(server);
    exporter.setNamingStrategy(new MBeanNamingStrategy(domain));
    exporter.setExportBehavior(ExportBehavior.FAIL_ON_EXISTING);
    exporter.setEnsureUnique(false);
    return exporter;
  }

  @Provides @Singleton
  protected MBeanServer mBeanServer() {
    return ManagementFactory.getPlatformMBeanServer();
  }
  

  static public class MBeanNamingStrategy implements JmxNamingStrategy {
    private String domain;
    
    MBeanNamingStrategy(String domain) {
      this.domain = domain;
    }
    
    @Override
    public ObjectName getObjectName(Object managedBean) throws MalformedObjectNameException {
      ObjectName objectName = getObjectNameInternal(managedBean);
      return objectName;
    }

    protected ObjectName getObjectNameInternal(Object managedBean) throws MalformedObjectNameException {
      Class<?> managedClass = managedBean.getClass();
      // check JmxSelfNaming
      if (managedBean instanceof JmxSelfNaming)
        return ((JmxSelfNaming) managedBean).getObjectName();
      // check annotation
      JmxBean jmxBean = managedClass.getAnnotation(JmxBean.class);
      if (jmxBean != null) {
        String oname = jmxBean.objectName();
        if(oname.length() == 0) oname = jmxBean.value();
        if (oname.length() > 0) {
          if(oname.indexOf(':') < 0) {
            oname = domain + ":" + oname;
          }
          return ObjectName.getInstance(oname);
        }
      }
      // default
      String packageName = managedClass.getPackage().getName();
      return ObjectName.getInstance(packageName + ":type=" + managedClass.getSimpleName());
    }
  }
}