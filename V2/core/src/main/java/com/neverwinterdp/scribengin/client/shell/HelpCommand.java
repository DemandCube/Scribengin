package com.neverwinterdp.scribengin.client.shell;

import static com.google.common.base.Strings.repeat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class HelpCommand extends Command {
  private int commandIndent = 30;
  private int subcommandIndent = commandIndent - 4;
  private int argIndent = subcommandIndent - 5;

  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    Map<String, Command> commands = shell.getCommands();
    shell.console().println("\n");
    shell.console().println("Available commands:");
    shell.console().println("==================");

    for (Map.Entry<String, Command> entry : commands.entrySet()) {
      if (cmdInput.getSubCommand() !=null && commands.keySet().contains(cmdInput.getSubCommand()))
        if (!entry.getKey().equals(cmdInput.getSubCommand())) {
          continue;
        }
      String commandName = entry.getKey();
      int length1 = commandName.length();
      shell.console().print("" + commandName + ":");
      shell.console().println(repeat(" ", commandIndent - length1) + entry.getValue().getDescription());
      for (Entry<String, Class<? extends SubCommand>> subCommands : entry.getValue().getSubcommands().entrySet()) {

        shell.console().print("     " + subCommands.getKey());
        shell.console().println(
            repeat(" ", subcommandIndent - subCommands.getKey().length())
                + subCommands.getValue().newInstance().getDescription());
        JCommander jcommander = new JCommander(subCommands.getValue().newInstance());
        List<ParameterDescription> params = jcommander.getParameters();
        if (params.size() > 0) {
          Collections.sort(params, new ParameterComparator());
          int length = 0;
          for (ParameterDescription parameterDescription : params) {
            length = parameterDescription.getNames().length();
            shell.console().print(repeat(" ", 8) + parameterDescription.getNames() + ": ");
            shell.console().println(
                repeat(" ", argIndent - length) + parameterDescription.getDescription());
          }
        }
      }
      System.out.println();
    }
    shell.console().println("\n");
  }

  @Override
  public String getDescription() {
    return "displays valid commands and their arguments.";
  }
  
  private class ParameterComparator implements java.util.Comparator<ParameterDescription>{

    @Override
    public int compare(ParameterDescription arg0, ParameterDescription arg1) {
          return arg0.getNames().compareTo(arg1.getNames());
    }    
  }  
}
