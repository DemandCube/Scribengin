package com.neverwinterdp.vm.client.shell;

abstract public class SubCommand {
  abstract public void execute(Shell shell, CommandInput cmdInput) throws Exception ;
}
