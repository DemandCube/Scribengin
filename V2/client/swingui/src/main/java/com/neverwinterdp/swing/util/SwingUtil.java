package com.neverwinterdp.swing.util ;

import java.awt.Component;
import java.awt.Container;
import java.util.ArrayList;
import java.util.List;

public class SwingUtil {

  static public <T extends Container> T findAncestorOfType(Container acomp, Class<T> type) {
    Container parent = acomp;
    while (parent != null) {
      if (type.isInstance(parent)) {
        return type.cast(parent);
      }
      parent = parent.getParent();
    }
    return null;
  }

  static public <T> T findDescentdantOfType(Container acomp, Class<T> type) {
    if (type.isInstance(acomp)) {
      return type.cast(acomp);
    }
    for (int i = 0; i < acomp.getComponentCount(); i++) {
      Component child = acomp.getComponent(i);
      if (type.isInstance(child)) {
        return type.cast(child);
      }
      if (child instanceof Container) {
        T found = findDescentdantOfType((Container) child, type);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  static public <T> List<T> findDescentdantsOfType(Container acomp, Class<T> type, boolean selfInclude) {
    List<T> holder = new ArrayList<T>();
    findDescentdantsOfType(holder, acomp, type, selfInclude);
    return holder;
  }

  static public <T> void findDescentdantsOfType(List<T> holder, Container acomp, Class<T> type, boolean selfInclude) {
    if (selfInclude && type.isInstance(acomp)) {
      holder.add((T) acomp);
    }
    for (int i = 0; i < acomp.getComponentCount(); i++) {
      Component child = acomp.getComponent(i);
      if (child instanceof Container) {
        findDescentdantsOfType(holder, (Container) child, type, true);
      }
    }
  }

  static public Component findDescentdantByName(Component acomp, String name) {
    if (name.equals(acomp.getName())) {
      return acomp;
    }
    if (!(acomp instanceof Container)) {
      return null;
    }

    Container container = (Container) acomp;
    for (int i = 0; i < container.getComponentCount(); i++) {
      Component child = container.getComponent(i);
      if (name.equals(child.getName())) {
        return child;
      }

      if (child instanceof Container) {
        Component found = findDescentdantByName(child, name);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }
}