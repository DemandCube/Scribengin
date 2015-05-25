package com.neverwinterdp.util.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class AnnotationUtil {
  static public <T extends Annotation> void invokeAnnotatedMethod(Object instance, Class<T> annotatedClass, Object[] args) throws Exception {
    Method[] method = instance.getClass().getMethods() ;
    for(Method selMethod : method) {
      Annotation annotation = selMethod.getAnnotation(annotatedClass) ;
      if(annotation != null) {
        selMethod.invoke(instance, args) ;
      }
    }
  }
  
  static public <T extends Annotation> List<Method> findAnnotatedMethod(Object instance, Class<T> annotatedClass) throws Exception {
    Method[] method = instance.getClass().getMethods() ;
    List<Method> holder = new ArrayList<Method>() ;
    for(Method selMethod : method) {
      T annotation = selMethod.getAnnotation(annotatedClass) ;
      if(annotation != null) {
        holder.add(selMethod) ;
      }
    }
    return holder ;
  }
  
  static public <T extends Annotation> List<Class<?>> findAnnotatedClasses(String pkg, Class<T> annotatedClass) throws Exception {
    List<Class<?>> holder = new ArrayList<Class<?>>() ;
    Package pkgObj = Package.getPackage(pkg);
    Package[] children = pkgObj.getPackages() ;
    
    return holder ;
  }
  
  static public void main(String[] args) {
    Package pkgObj = Package.getPackage("com.neverwinterdp.util");
    System.out.println(pkgObj.getName());
    Package[] pkgs = Package.getPackages() ;
    for(Package sel : pkgs) {
      System.out.println(sel.getName());
    }
  }
}