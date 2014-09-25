Vagrant file to use.  Note - replace vm.synced_folder with /path/to/your/workspace/that/contains/Scribengin
```
# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  

  config.vm.define :kxae do | kxae |
    kxae.vm.box = "demandcube/centos-65_x86_64-VB-4.3.8"
    
    # Create a private network
    kxae.vm.network :private_network, ip: "192.168.1.2", :netmask => "255.255.0.0" 
    kxae.vm.hostname = "kxae"
    kxae.vm.synced_folder  "/Users/rcduar/workspace", "/vagrant"
    
    config.vm.provider :virtualbox do |vb|
      vb.name = "kxae"
      vb.customize ["modifyvm", :id, "--memory", "6168"]
      vb.customize ["modifyvm", :id, "--cpus", "2"]
    end
  end
end
```
