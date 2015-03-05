package com.neverwinterdp.vm.client.shell;

import static com.google.common.base.Strings.repeat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;

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
      String line;
      if (cmdInput.getSubCommand() != null && commands.containsKey(cmdInput.getSubCommand())) {
        if (entry.getKey().equals(cmdInput.getSubCommand())) {
          line = printSubCommand(entry);
          shell.console().println(line);
        }
      } else {
        line = printSubCommand(entry);
        shell.console().println(line);
      }
    }
  }

  private String printSubCommand(Map.Entry<String, Command> entry) throws Exception {

    StringBuilder builder = new StringBuilder();
    String commandName = entry.getKey();
    int length1 = commandName.length();
    builder.append("" + commandName + ":");
    builder.append(repeat(" ", commandIndent - length1) + entry.getValue().getDescription())
        .append("\n");
    for (Entry<String, Class<? extends SubCommand>> subCommands : entry.getValue().getSubcommands()
        .entrySet()) {
      builder.append("     " + subCommands.getKey());
      builder.append(repeat(" ", subcommandIndent - subCommands.getKey().length())
          + subCommands.getValue().newInstance().getDescription()).append("\n");
      JCommander jcommander = new JCommander(subCommands.getValue().newInstance());
      List<ParameterDescription> params = jcommander.getParameters();

      if (params.size() > 0) {
        Collections.sort(params, new ParameterComparator());

        int length = 0;
        for (ParameterDescription parameterDescription : params) {
          builder.append(repeat(" ", 8) + parameterDescription.getNames() + ": ");
          length = parameterDescription.getNames().length();
          builder.append(repeat(" ", argIndent - length) + parameterDescription.getDescription())
              .append("\n");
        }
      }
    }
    builder.append("\n");
    return builder.toString();
  }

  @Override
  public String getDescription() {
    return "displays valid commands and their arguments.";
  }

  private class ParameterComparator implements java.util.Comparator<ParameterDescription> {
    @Override
    public int compare(ParameterDescription arg0, ParameterDescription arg1) {
      return arg0.getNames().compareTo(arg1.getNames());
    }
  }
}
