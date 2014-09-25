First things first.  Check out NeverwinterDP on your local machine
```
cd /path/to/your/workspace/
git clone http://github.com/DemandCube/NeverwinterDP
cd NeverwinterDP
./neverwinterdp.sh checkout
```

Vagrant file to use.  Note - replace vm.synced_folder with /path/to/your/workspace/
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

Run (This is to make sure your proxy will work in the next steps) :
```
vagrant up
ssh -D 9999 vagrant@192.168.1.2
#Password: vagrant
```

Once on your vagrant machine:
```
git clone https://github.com/DemandCube/hadoop-single-node-cluster
cd hadoop-single-node-cluster
sudo ./INSTALL-HADOOP
#This will take a long time. Go get some coffee
```

On your host machine: to watch the process of installation with Ambari, you'll need to install foxyproxy in FireFox or Chrome:
```
Click on the fox icon next to your url bar. Click on Add new Proxy. Under proxy details, select manual. 

Host: localhost 
Port: 9999
Enable 'Socks Proxy'
Click "OK" to save.  Once saved - Change the mode to "Use proxy localhost:9999 for all URLs (or you can figure out how to make a pattern subscription)
```

Back on the vagrant machine, lets install Gradle 1.12 to our home directory while we wait
```
cd ~/
wget https://services.gradle.org/distributions/gradle-1.12-bin.zip
unzip gradle-1.12-bin.zip
echo "export PATH=$PATH:/home/vagrant/gradle-1.12/bin/" >> ~/.bashrc
source ~/.bashrc
```

We now need to set up and launch kafka  (Only launch once zookeeper is running in the Ambari console)
```
wget https://archive.apache.org/dist/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz
tar -xzf kafka_2.8.0-0.8.0.tar.gz
cd kafka_2.8.0-0.8.0
./bin/kafka-server-start.sh ./config/server.properties
```

You'll now need to make a topic in Kafka for Scribengin to consume
```
cd kafka_2.8.0-0.8.0 #Go back to your kafka directory
./bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 partition 1 --topic [YOUR TOPIC NAME GOES HERE]
```


Now lets build Scribengin - to do this, lets use our handy dandy Neverwinter scripts
```
#Installing java is a dependency for this step
sudo yum install java-1.7.0-openjdk java-1.7.0-openjdk-devel

#This is the folder that's mounted from your host machine via the Vagrantfile
cd /vagrant/NeverwinterDP
./neverwinterdp.sh gradle clean build install
```
