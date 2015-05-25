package com.neverwinterdp.vm.client.shell;

import static com.google.common.base.Strings.repeat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.neverwinterdp.util.text.TabularFormater;

public class HelpCommand extends Command {
  private int indent = 2;
  //private int commandIndent = 30;
  //private int subcommandIndent = commandIndent - 4;
  private int argIndent = 20;

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
  
  private String wordWrap(String toWrap, int indents){
    return wordWrap(toWrap, indents, 75);
  }
  
  private String wordWrap(String toWrap, int indents, int lineLimit){
    StringBuilder sb = new StringBuilder(toWrap);
    int i = 0;
    while (i + lineLimit < sb.length() && (i = sb.lastIndexOf(" ", i + lineLimit)) != -1) {
      sb.replace(i, i + 1, "\n"+repeat(" ", indents));
        //To avoid any sort of string that might kill this
        if(i > 10000){
          return toWrap;
        }
    }
    return sb.toString();
  }
  
  private String indent(String toIndent, int indents){
    StringBuilder sb = new StringBuilder();
    for(String x: toIndent.split("\n")){
      sb.append(repeat(" ", indents)+ x + "\n");
    }
    return sb.toString();
  }

  private String printSubCommand(Map.Entry<String, Command> entry) throws Exception {

    StringBuilder builder = new StringBuilder();
    String commandName = entry.getKey();
    builder.append("" + commandName + ":\n");
    builder.append(repeat(" ", indent) + wordWrap(entry.getValue().getDescription(), indent*1))
        .append("\n\n");
    for (Entry<String, Class<? extends SubCommand>> subCommands : entry.getValue().getSubcommands()
        .entrySet()) {
      builder.append(repeat(" ",indent)).append("* " + subCommands.getKey());
      builder.append("\n").append(repeat(" ", indent*2)+
          wordWrap(subCommands.getValue().newInstance().getDescription(), indent*2)).append("\n\n");
      JCommander jcommander = new JCommander(subCommands.getValue().newInstance());
      List<ParameterDescription> params = jcommander.getParameters();
      if (params.size() > 0) {
        Collections.sort(params, new ParameterComparator());
        TabularFormater formatter = null;
        for (ParameterDescription parameterDescription : params) {
          int length = 0;
          //builder.append(repeat(" ", indent*3) + parameterDescription.getNames() + ": ");
          length = parameterDescription.getNames().length();
          int thisargsindent = argIndent - length;
          if(thisargsindent <= 0){
            thisargsindent = 1;
          }
          //builder.append(wordWrap(repeat(" ", thisargsindent)+parameterDescription.getDescription(),argIndent+8))
          //    .append("\n");
          formatter = new TabularFormater("Option", "Description", "Default Value");
          try{
            formatter.addRow(parameterDescription.getNames().trim(),
                          parameterDescription.getDescription().trim(),
                          parameterDescription.getDefault());
          } catch (Exception e){
            formatter.addRow(parameterDescription.getNames().trim(),
                parameterDescription.getDescription().trim(),
                "");
          }
          
        }
        if(formatter != null){
          builder.append(indent(formatter.getFormatText(), indent*4));
        }
        builder.append("\n");
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
