package com.neverwinterdp.vm.client.shell;
//TODO  anthony investigate remove getDescription
abstract public class SubCommand {
  abstract public void execute(Shell shell, CommandInput cmdInput) throws Exception ;
  abstract public String getDescription();
}
