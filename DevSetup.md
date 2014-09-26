First things first.  Check out NeverwinterDP on your local machine
```
cd /path/to/your/workspace/
git clone http://github.com/DemandCube/NeverwinterDP
cd NeverwinterDP
./neverwinterdp.sh checkout
```

Here's the Vagrantfile to use.  
#####Note - replace vm.synced_folder with /path/to/your/workspace/
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
You can now navigate to http://localhost.localdomain:8080 and see the Ambari console
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
sudo yum install wget
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
cd ../Scribengin
```

Edit what user you run as when you work with Hadoop
```
#This is to avoid permission problems
echo "export HADOOP_USER_NAME=hdfs" >> ~/.bashrc
source ~/.bashrc
```

Almost there!  Copy your jar file to HDFS
```
hdfs dfs -copyFromLocal /vagrant/Scribengin/bin/build/libs/scribengin-1.0-SNAPSHOT.jar /
```

Run Scribengin
```
/usr/lib/hadoop/bin/hadoop jar /vagrant/Scribengin/bin/build/libs/scribengin-1.0-SNAPSHOT.jar --container_mem 300 --am_mem 300 --container_cnt 1  --hdfsjar /scribengin-1.0-SNAPSHOT.jar --app_name foobar --command echo --am_class_name "com.neverwinterdp.scribengin.yarn.ScribenginAM" --topic [YOUR TOPIC NAME GOES HERE] --kafka_seed_brokers 10.0.2.15:9092
#Notice that the --container_cnt and --command options no longer make sense. They will be removed soon
```



To see it running:
------------------
- Go to Ambari (Use FoxyProxy and navigate to (http://localhost.localdomain:8080). 
- Click on Yarn on the left. 
- Click on the quick link drop down in the middle and select ResourceManager UI.
- The resouremanager ui will tell you what's running, what failed, etc. You can click around to get to the application master log. You can't get to the worker log from here, however.
- To get to the worker log, go back to your vm, and cd into /hadoop/yarn/log/[application_id]/[container]_0000[x]. You can find the application id from the resource manager web ui. Container_000001 is your app master. >1 are your workers. Cd into that, and you should see stdout and std error.


To kill the job:
----------------
Ctrl-c doesn't really kill the yarn app. You'll need to issue a kill command.
yarn application -kill  [application_id]
