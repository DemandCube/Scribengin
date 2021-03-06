#Design#

The structure directory of the scribengin docker

````
  Dockerfile 
  docker.sh
  bootstrap/
    post-install
      cluster.sh      
      post-install.sh 
      ssh-config/
         [...]
      hadoop/     
         [...]
      kafka/           
         [...]
      zookeeper/
         [...]
  README.md 
````

#####Dockerfile

The Dockerfile contains the commands to build an ubuntu images with the required components.

1. Use apt-get command to install  openssh, open jdk, vim... 
2. Reconfigure the root ssh access
3. Download the java opensource project such hadoop, zookeeper, kafka... and extract to the /opt directory
4. Copy the bootstrap/post-install to the /tmp directory
5. Run the script /tmp/post-install/post-install.sh. Due to the file security bug in the docker, certain commands cannot be performed in the Dockerfile and need to be run in the post-install.sh script.

#####The bootstrap/post-install/post-install.sh file

The post-install.sh script run the commands to:

1. Creathe the neverwinterdp user and configure the neverwinterdp user ssh access with the ssh-config template
2. Patch the hadoop, zookeeper, kafka configuration with the configuration in post-install directory
3. Copy the cluster.sh script to /opt directory



#####The docker.sh file

````
The cluster.sh script options: 
Command image consists of the sub commands: 
build                 : To build the ubuntu os image with the required components
clean                 : To remove the image
Command container consists of the sub commands: 
run                   : To run the containers(hadoop, zookeeper, kafka...)
  clean                 : To remove and destroy all the running containers
  login                 : To login the given containeri name or id  with the root user
  update-hosts          : To update the /etc/hosts in all the running containers
  Other commands:
  ssh                   : The ssh command use to resolve the container ssh port and login a container with ssh command
  scp                   : The scp command use to resolve the container ssh port and copy the file/directory from or to a container
  update-hosts		: The update-hosts use to update /etc/hosts file of the host machine with docker containers details
````

#To build the Docker cluster and run Scribengin#

#####Get Scribengin
```
git clone http://github.com/DemandCube/Scribengin
```

#####Dependencies
1. [Install Docker according to the documentation](https://docs.docker.com/installation/)
2. Make sure git is set up
3. Run the script to install other DemandCube dependencies
  
  ```
  cd pathTo/Scribengin/V2
  ./installDependencies.sh
  ```

#####Build and release Scribengin
```
cd pathTo/Scribengin/V2/docker/scribengin/
./scribengin.sh build
```

#####To build the linux os image
Before you run, if you have ssh keys set up on your system already, you may want to consider removing the password on them.  It will make your life easier while deploying.
```
ssh-keygen -p
# Start the SSH key creation process
# Enter file in which the key is (/Users/you/.ssh/id_rsa): [Hit enter]
# Key has comment '/Users/you/.ssh/id_rsa'
# Enter new passphrase (empty for no passphrase): [Type new passphrase]
# Enter same passphrase again: [One more time for luck]
# Your identification has been saved with the new passphrase.
```

````
cd path-to/Scribengin/V2/docker/scribengin
$./docker.sh image clean
$./docker.sh image build
````

#####To launch all the required vm for the scribengin 

````
$./docker.sh container clean
$./docker.sh container run
````

This command will:

1. Launch a hadoop cluster with 1 vm for the hadoop master , 3 vm for the hadoop data nodes
2. Launch a vm for the zookeeper
3. Launch 3 vm for the kafka

The command also run the update-hosts command to update the /etc/hosts of all the launched conatainers

You can run the command 'docker ps' to make sure that all those containers are launched

#####Editing your /etc/hosts file

The below command will update host machines /etc/hosts with the hostnames and ip of the containers.
Note: Run this command as root or sudo user.

For root user
````
$./docker.sh update-hosts
````
For sudo user
````
$sudo ./docker.sh update-hosts
````

If you use the MAC OS, you need to route the ip
````
  ./docker.sh ip-route
````

#####Login to any containers

You may login to any containers with the neverwinterdp user. The password of the neverwinterdp user is **'neverwinterdp'**

````
$./docker.sh ssh neverwinterdp@hadoop-master
#Password is 'neverwinterdp'
````

#####The bootstrap/post-install/cluster.sh file

The cluster.sh contains several command and sub commands to launch and manage the different components of the cluster

#####To run scribengin and the required components **(while logged into hadoop-master)**

We need to go to the /opt directory and run the ./cluster start --clean command

````
$cd /opt
$./cluster.sh start --clean
````

#####To access the webui:

1. To access the dfs webui http://${docker.host.ip}:50070
1. To access the yarn webui http://${docker.host.ip}:8088, you should see the scribengin master running here(But it is in the development)

#####To stop the cluster

````
  ./cluster.sh stop
````

But I think currently there is a bug with zookeeper or kafka shutdown script. You better use the command
````
  ./cluster.sh exec "pkill -9 java"
````
This command will kill all the java processes on all the  machines


#Build and run Scribengin#

Build the scribengin code

````
 cd path-to/Scribengin/V2/docker/scribengin
 ./scribengin.sh build
````

To package the scribengin code

````
 cd path-to/Scribengin/V2/release
 gradle release -x test
````

*To run the scribengin we will need:*

Launch the dependencies such zookeeper , kafka, hadoop. The script

````
  cd path-to/Scribengin/V2/docker/scribengin/bootstrap/post-install/
  ./cluster.sh start --clean
````

will help to launch the zookeeper , kafka and hadoop.

*Launch the scribengin application*

This command will launch the yarn vm framework

````
  cd V2/release/build/release/scribengin
  ./bin/shell.sh vm start
````

Check http://hadoop-master:8088/cluster address with a browser, you should see the vm-master-1  with the RUNNING status

This command will launch the scribengin application

````
  ./bin/shell.sh scribengin start
````

You can check the scribengin status by running the command 
````
  ./bin/shell.sh vm info
````
and 
````
  ./bin/shell.sh scribengin info
````


*Submit a hello hdfs dataflow*

Create hdfs data source that will be used by the hello hdfs dataflow

````
  ./bin/shell.sh dataflow hdfs --create-source
````

Check http://hadoop-master:50070 and go to Utilities > Browse the file system, you should find the data is created in /data/source directory

Submit the dataflow

````
  ./bin/shell.sh dataflow submit --deploy ../dataflows/hdfs --descriptor ../dataflows/hdfs/dataflow.json
````

If the dataflow is submitted successfully, you should see the status and registry structure print out on the console

*To redeploy Scribengin to the cluster*
```
 cd path-to/Scribengin/V2/docker/scribengin
 ./scribengin.sh deploy --clean
```
