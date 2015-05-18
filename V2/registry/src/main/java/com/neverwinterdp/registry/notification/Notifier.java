package com.neverwinterdp.registry.notification;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.neverwinterdp.registry.DataMapperCallback;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.JSONSerializer;

public class Notifier {
  final static public SimpleDateFormat COMPACT_DATE_TIME_ID = new SimpleDateFormat("dd-MM-yyyy@HH:mm:ss") ;

  final static public DataMapperCallback<NotificationEvent> MAPPER = new DataMapperCallback<NotificationEvent>() {
    @Override
    public NotificationEvent map(String path, byte[] data, Class<NotificationEvent> type) {
      int idx = path.lastIndexOf('-') ;
      String seqId = path.substring(idx + 1, path.length());
      NotificationEvent event = JSONSerializer.INSTANCE.fromBytes(data, type);
      event.setSeqId(seqId);
      return event;
    }
  };
  
  final static public Comparator<NotificationEvent> COMPARATOR = new Comparator<NotificationEvent>() {
    @Override
    public int compare(NotificationEvent e1, NotificationEvent e2) {
      return e1.getSeqId().compareTo(e2.getSeqId());
    }
    
  };
  
  private Node     eventsNode ;
  
  public Notifier() {} 
  
  public Notifier(Registry registry, String path, String name) throws RegistryException {
    init(registry, path, name);
  }
  
  protected void init(Registry registry, String path, String name) throws RegistryException {
    eventsNode = registry.createIfNotExist(path + "/" + name + "-events") ;
  }
  
  public void info(String name, String mesg) throws RegistryException {
    NotificationEvent notificationEvent = new NotificationEvent(NotificationEvent.Level.INFO, name, mesg) ;
    eventsNode.createChild("info-" + name + "-", notificationEvent, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public void error(String name, String mesg) throws RegistryException {
    NotificationEvent notificationEvent = new NotificationEvent(NotificationEvent.Level.ERROR, name, mesg) ;
    eventsNode.createChild("error-" + name + "-", notificationEvent, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public void error(String name, String mesg, Throwable t) throws RegistryException {
    String stacktrace = ExceptionUtil.getStackTrace(t);
    String errorText = mesg + "\n" + stacktrace; 
    NotificationEvent notificationEvent = new NotificationEvent(NotificationEvent.Level.ERROR, name, errorText) ;
    eventsNode.createChild("error-" + name + "-", notificationEvent, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public void warn(String name, String mesg) throws RegistryException {
    NotificationEvent notificationEvent = new NotificationEvent(NotificationEvent.Level.WARNING, name, mesg) ;
    eventsNode.createChild("warn-" + name + "-", notificationEvent, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public void warn(String name, String mesg, Throwable t) throws RegistryException {
    String stacktrace = ExceptionUtil.getStackTrace(t);
    String errorText = mesg + "\n" + stacktrace; 
    NotificationEvent notificationEvent = new NotificationEvent(NotificationEvent.Level.WARNING, name, errorText) ;
    eventsNode.createChild("warn-" + name + "-", notificationEvent, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  static public List<NotificationEvent> getNotificationEvents(Registry registry, String path) throws RegistryException {
    List<NotificationEvent> holder = registry.getChildrenAs(path,NotificationEvent.class, MAPPER) ;
    Collections.sort(holder, COMPARATOR);
    return holder ;
  }
}

