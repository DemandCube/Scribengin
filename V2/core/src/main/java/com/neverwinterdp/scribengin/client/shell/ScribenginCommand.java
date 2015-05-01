package com.neverwinterdp.scribengin.client.shell;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class ScribenginCommand extends Command {
  public ScribenginCommand() {
    add("start", Start.class);
    add("shutdown", Shutdown.class);
    add("info", Info.class);
    add("master", Master.class);
  }

  static public class Start extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      ScribenginClusterBuilder clusterBuilder = new ScribenginClusterBuilder(new VMClusterBuilder(vmClient));
      clusterBuilder.startScribenginMasters();
    }

    @Override
    public String getDescription() {
      return "start a scribenging cluster";
    }
  }

  static public class Shutdown extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell) shell).getScribenginClient();
      client.shutdown();
    }

    @Override
    public String getDescription() {
      return "shutdown a scribengin cluster.";
    }
  }

  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell) shell).getScribenginClient();
      shell.console().h1("Scribengin Info");
      Formater.DataflowList runningDflFormater = new Formater.DataflowList(client.getActiveDataflowDescriptors());
      shell.console().println(runningDflFormater.format("Running Dataflows"));
      Formater.DataflowList historyDflFormater = new Formater.DataflowList(client.getHistoryDataflowDescriptors());
      shell.console().println(historyDflFormater.format("History Dataflows"));
    }

    @Override
    public String getDescription() {
      return "get info about the scribengin cluster";
    }
  }
  static public class Master extends SubCommand {

    @Parameter(names = "--list", description = "List all running scribengin masters")
    private boolean list;

    @Parameter(names = "--shutdown", description = "Shutdown current master")
    private boolean shutdown;

    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell) shell).getScribenginClient();
      String leaderPath = client.getScribenginMaster().getRegistryPath();

      if (list) {
        shell.console().h1("Listing Scribengin Masters");
        Formater.VmList formatter = new Formater.VmList(client.getScribenginMasters(), leaderPath);
        shell.console().println(formatter.format("Scribengin Masters"));
      } else if (shutdown) {
        // shell.console().h1("Shutting down current Scribengin Master");
        VMClient vmClient = shell.getVMClient();
        for (VMDescriptor desc : vmClient.getActiveVMDescriptors()) {
          if (desc.getRegistryPath().equals(leaderPath)) {
            shell.console().h1("Shutting down leader " + desc.getId());
            vmClient.shutdown(desc);
            Thread.sleep(20000);
          }
        }
      } else {
        shell.console().println("Please provide either --shutdown or --list");
      }
    }

    @Override
    public String getDescription() {
      return "commands for managing the Scribengin master.";
    }
  }
  @Override
  public String getDescription() {
    return "commands for interacting with the scribengin cluster.";
  }
}
