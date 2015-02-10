package com.neverwinterdp.vm.client.shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class CommandInput {
  private String commandLine ;
  private String command ;
  private String subCommand;
  private String[] remainArgs ;
  
  public CommandInput(String cmdLine) {
    this(cmdLine, true) ;
  }
  
  public CommandInput(String cmdLine, boolean parseSubCommand) {
    this(parseArgs(cmdLine), parseSubCommand);
    this.commandLine = cmdLine ;
  }
  
  public CommandInput(String[] args, boolean parseSubCommand) {
    command = args[0] ;
    args = shift(args) ;
    if(args == null) return ;
    if(parseSubCommand && !args[0].startsWith("-")) {
      subCommand = args[0] ;
      args = shift(args) ;
    }
    if(args == null) return ;
    remainArgs = args ;
  }
  
  public String getCommandLine() { return this.commandLine ; }
  
  public String getCommand() { return this.command ; }
  public void setCommand(String command) {
    this.command = command ;
  }
  
  public String getSubCommand() { return this.subCommand ; }
  
  public void setSubCommand(String cmd) {
    this.subCommand = cmd ;
  }
  
  public String[] getRemainArgs() { return this.remainArgs ; }
  
  public <T> void mapRemainArgs(T object) {
    //JCommander jcommander = new JCommander(object, this.remainArgs) ;
    ParameterMapper mapper = new ParameterMapper() ;
    Arrays.asList(remainArgs);
    List<String> remainList = mapper.map(object, Arrays.asList(remainArgs)) ;
    remainArgs = remainList.toArray(new String[remainList.size()]);
  }
  
  static public String[] shift(String[] array){
    if(array == null || array.length == 0) return null ;
    String[] newArray = new String[array.length - 1] ;
    System.arraycopy(array, 1, newArray, 0, newArray.length);
    return newArray ;
  }
  
  static public String[] parseArgs(String line) {
    List<String> holder = new ArrayList<String>();
    //parse data format in  #data{ }#
    String data = null ;
    if(line.indexOf("#{data") > 0 && line.endsWith("}#")) {
      int idx = line.indexOf("#{data") ;
      data = line.substring(idx + 6 , line.length() - 2) ;
      line = line.substring(0, idx) ;
    }
    Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
    while (m.find()) {
      String arg = m.group(1).trim() ;
      if(arg.length() == 0) continue ;
      //Add .replace("\"", "") to remove surrounding quotes.
      if(arg.startsWith("\"") && arg.endsWith("\"")) {
        arg = arg.substring(1, arg.length() - 1) ;
      }
      holder.add(arg); 
    }
    if(data != null) holder.add(data) ;
    return holder.toArray(new String[holder.size()]) ;
  }
  
  static class ParameterMapper {
    @ParametersDelegate
    Object object;
    
    @Parameter(description = "main parameter")
    private List<String> mainParameters;
    
    public List<String> map(Object object, List<String> argsList) {
      this.object = object ;
      mainParameters = new ArrayList<String>();
      String[] args = argsList.toArray(new String[argsList.size()]) ;
      JCommander jcommander = new JCommander(this) ;
      jcommander.setAcceptUnknownOptions(true);
      jcommander.parse(args);
      List<String> remainOptions = new ArrayList<String>() ;
      remainOptions.addAll(jcommander.getUnknownOptions()) ;
      remainOptions.addAll(mainParameters) ;
      return remainOptions ;
    }
  }
}