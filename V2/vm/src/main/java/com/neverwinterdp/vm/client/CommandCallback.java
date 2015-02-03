package com.neverwinterdp.vm.client;

import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

public interface CommandCallback {
  public void onResponse(Command command, CommandResult<?> result);
}
