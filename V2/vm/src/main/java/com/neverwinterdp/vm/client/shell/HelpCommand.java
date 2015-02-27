package com.neverwinterdp.vm.client.shell;

import java.util.Map;

public class HelpCommand extends Command {
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    Map<String, Command> commands = shell.getCommands();
    shell.console().println("\n");
    shell.console().println("Available command:");
    shell.console().println("==================");
    for(Map.Entry<String, Command> entry : commands.entrySet()) {
      String commandName = entry.getKey();
      shell.console().println(commandName + ":");
    }
    shell.console().println("\n");
  }
}
