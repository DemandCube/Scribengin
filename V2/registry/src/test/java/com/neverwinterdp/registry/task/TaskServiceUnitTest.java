package com.neverwinterdp.registry.task;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class TaskServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TASKS_PATH = "/tasks";
  
  static private EmbededZKServer zkServerLauncher ;

  private Injector container ;
  private Registry registry ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Before
  public void setup() throws Exception {
    AppModule module = new AppModule(new HashMap<String, String>()) {
      @Override
      protected void configure(Map<String, String> properties) {
        bindInstance(RegistryConfig.class, RegistryConfig.getDefault());
        bindType(Registry.class, RegistryImpl.class);
      }
    };
    container = 
      Guice.createInjector(Stage.PRODUCTION, new CloseableModule(), new Jsr250Module(), module);
    registry = container.getInstance(Registry.class);
  }
  
  @After
  public void teardown() throws Exception {
    registry.rdelete(TASKS_PATH);
    registry.disconnect();
    container.getInstance(CloseableInjector.class).close();
  }

  @Test
  public void testTaskService() throws Exception {
    TaskService<TaskDescriptor> service = new TaskService<>(registry, TASKS_PATH, TaskDescriptor.class);
    TestTaskMonitor<TaskDescriptor> monitor = new TestTaskMonitor<TaskDescriptor>();
    service.addTaskMonitor(monitor);
    
    int NUM_OF_TASKS = 100;
    DecimalFormat seqIdFormater = new DecimalFormat("000");
    for(int i = 0; i < NUM_OF_TASKS; i++) {
      String taskId = "task-" + seqIdFormater.format(i) ;
      service.offer(taskId, new TaskDescriptor(taskId));
    }
    
    try {
      service.offer("task-000", new TaskDescriptor("task-000"));
      Assert.fail("should fail since the task-000 is already created");
    } catch(RegistryException ex) {
      Assert.assertEquals(ErrorCode.NodeExists, ex.getErrorCode());
    }
    service.getTaskRegistry().getTasksRootNode().dump(System.out);

    int NUM_OF_EXECUTORS = 20;
    ExecutorService execService = Executors.newFixedThreadPool(NUM_OF_EXECUTORS);
    for(int i = 0; i < NUM_OF_EXECUTORS; i++) {
      TaskExecutor<TaskDescriptor> executor = new TaskExecutor<>(i + "", service);
      execService.submit(executor);
    }
    execService.shutdown();
    execService.awaitTermination(30000, TimeUnit.MILLISECONDS);
    service.getTaskRegistry().getTasksRootNode().dump(System.out);
    service.onDestroy();
    Assert.assertEquals(NUM_OF_TASKS, monitor.finishCounter);
  }
  
  final static public class TaskDescriptor {
    private String description;

    public TaskDescriptor() { }
    
    public TaskDescriptor(String desc) {
      this.description = desc;
    }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
  }
  
  static public class TestTaskMonitor<T> implements TaskMonitor<T> {
    int finishCounter = 0;
    
    @Override
    public void onAssign(TaskContext<T> context) {
      System.out.println("on assign task " + context.getTaskId());
    }

    @Override
    public void onAvailable(TaskContext<T> context) {
      System.out.println("on available task " + context.getTaskId());
    }

    @Override
    public void onFinish(TaskContext<T> context) {
      finishCounter++ ;
      System.out.println("on finish task " + context.getTaskId());
    }
    
  }
  
  static public class TaskExecutor<T> implements Runnable {
    private String id;
    private TaskService<T> taskService ;
    
    public TaskExecutor(String id, TaskService<T> taskService ) {
      this.id = id;
      this.taskService = taskService;
    }
    
    public void run() {
      TaskContext<T> tContext = null ;
      try {
        Random rand = new Random();
        while((tContext = taskService.take(id)) != null) {
          long processTime = rand.nextInt(500) + 1;
          Thread.sleep(processTime);
          if(rand.nextInt(5)  % 3 == 0) {
            taskService.finish(id, tContext.getTaskId());
          } else {
            taskService.suspend(id, tContext.getTaskId());
          }
        }
      } catch(InterruptedException ex) {
      } catch(RegistryException ex) {
        ex.printStackTrace();
      }
    }
  }
}