package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

/*
 * scribengin
  master
    leader - {"storedPath":"/vm/allocated/vm-scribengin-master-1","memory":128,"cpuCores":
      leader-0000000000
      leader-0000000001
  dataflows
    test-dataflow - {"name":"test-dataflow","sourceDescriptor":{"location":"./build/hdfs/source",
      tasks
        locks
        assigned
        finished
          task-0000000008 - {"id":8,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000009 - {"id":9,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000012 - {"id":12,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScri
          task-0000000003 - {"id":3,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000002 - {"id":2,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000011 - {"id":11,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScri
          task-0000000014 - {"id":14,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScri
          task-0000000001 - {"id":1,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000013 - {"id":13,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScri
          task-0000000000 - {"id":0,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000007 - {"id":7,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000006 - {"id":6,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000005 - {"id":5,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000004 - {"id":4,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScrib
          task-0000000010 - {"id":10,"status":"INIT","dataProcessor":"com.neverwinterdp.scribengin.VMScri
        available
      workers
        test-dataflow-worker-2 - {"storedPath":"/vm/allocated/test-dataflow-worker-2","memory":128,"cpuCores":
        test-dataflow-worker-1 - {"storedPath":"/vm/allocated/test-dataflow-worker-1","memory":128,"cpuCores":
        test-dataflow-worker-3 - {"storedPath":"/vm/allocated/test-dataflow-worker-3","memory":128,"cpuCores":
      master
        leader - {"storedPath":"/vm/allocated/test-dataflow-master-1","memory":128,"cpuCores":
          leader-0000000000
vm
  history
  allocated
    test-dataflow-master-1 - {"storedPath":"/vm/allocated/test-dataflow-master-1","memory":128,"cpuCores":
      status - "RUNNING"
        heartbeat
      commands
    vm-scribengin-master-1 - {"storedPath":"/vm/allocated/vm-scribengin-master-1","memory":128,"cpuCores":
      status - "RUNNING"
        heartbeat
      commands
    vm-master-1 - {"storedPath":"/vm/allocated/vm-master-1","memory":128,"cpuCores":1,"hostname
      status - "RUNNING"
        heartbeat
      commands
    vm-scribengin-master-2 - {"storedPath":"/vm/allocated/vm-scribengin-master-2","memory":128,"cpuCores":
      status - "RUNNING"
        heartbeat
      commands
    test-dataflow-worker-2 - {"storedPath":"/vm/allocated/test-dataflow-worker-2","memory":128,"cpuCores":
      status - "RUNNING"
        heartbeat
      commands
    test-dataflow-worker-1 - {"storedPath":"/vm/allocated/test-dataflow-worker-1","memory":128,"cpuCores":
      status - "RUNNING"
        heartbeat
      commands
    test-dataflow-worker-3 - {"storedPath":"/vm/allocated/test-dataflow-worker-3","memory":128,"cpuCores":
      status - "RUNNING"
        heartbeat
      commands
  master
    leader - {"storedPath":"/vm/allocated/vm-master-1","memory":128,"cpuCores":1,"hostname
      leader-0000000000
 */
public class ScribenginCommand extends Command {
  public ScribenginCommand() {
    add("master", new ListMasterDescriptor()) ;
  }
  
  static public class ListMasterDescriptor extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
    }
  }
  
  static public class DataflowDeploy extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
    }
  }
}
