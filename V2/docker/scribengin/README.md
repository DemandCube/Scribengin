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

##The Dockerfile file##

The Dockerfile contains the commands to build an ubuntu images with the required components.

1. Use apt-get command to install  openssh, open jdk, vim... 
2. Reconfigure the root ssh access
3. Download the java opensource project such hadoop, zookeeper, kafka... and extract to the /opt directory
4. Copy the bootstrap/post-install to the /tmp directory
5. Run the script /tmp/post-install/post-install.sh. Due to the file security bug in the docker, certain commands cannot be performed in the Dockerfile and need to be run in the post-install.sh script.

##The bootstrap/post-install/post-install.sh file##

The post-install.sh script run the commands to:

1. Creathe the neverwinterdp user and configure the neverwinterdp user ssh access with the ssh-config template
2. Patch the hadoop, zookeeper, kafka configuration with the configuration in post-install directory
3. Copy the cluster.sh script to /opt directory



##The docker.sh file##

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
````

#To build and run scribengin with docker#

##To build the linux os image

````
$./docker.sh image clean  && ./docker.sh image build
````

##To launch all the required vm for the scribengin 

````
$./docker.sh container clean && ./docker.sh container run
````

This command will:

1. Launch a hadoop cluster with 1 vm for the hadoop master , 3 vm for the hadoop data nodes
2. Launch a vm for the zookeeper
3. Launch 3 vm for the kafka

The command also run the update-hosts command to update the /etc/hosts of all the launched conatainers

You can run the command 'docker ps' to make sure that all those containers are launched

##Login to any containers##

You may login to any containers with the neverwinterdp user. The password of the neverwinterdp user is 'neverwinterdp'

````
$./docker.sh ssh neverwinterdp@hadoop-master
````

##The bootstrap/post-install/cluster.sh file##

The cluster.sh contains several command and sub commands to launch and manage the different components of the cluster

##To run the scribengin and the required componnents##

We need to go to the /opt directory and run the ./cluster start --clean command

````
$cd /opt
$./cluster.sh start --clean
````

To access the webui:

1. To access the dfs webui http://${docker.host.ip}:50070
1. To access the yarn webui http://${docker.host.ip}:8088, you should see the scribengin master running here(But it is in the development)

To stop the cluster

````
$./cluster.sh stop
````
#Run Scribengin#

Checkout Scribengin from github
````
  git pull
````

Go to Scribengin/V2/docker/scribengin
````
  cd path-to/Scribengin/V2/docker/scribengin
````

Build the docker image base on the latest ubuntu. Check the Dockerfile for more information
````
  ./docker.sh image build
````

Launch the containers to run hadoop, zookeeper, kafka
````
  ./docker.sh container run
````

The ./docker.sh container run command should print out the name and the information about the hostnames and ip of the containers. Copy those to /etc/hosts
````
#Copy this print out to your /etc/hosts
172.17.0.81 kafka-3
172.17.0.80 kafka-2
172.17.0.79 kafka-1
172.17.0.78 zookeeper
172.17.0.77 hadoop-worker-3
172.17.0.76 hadoop-worker-2
172.17.0.75 hadoop-worker-1
172.17.0.74 hadoop-master
````

If you use the MAC OS, you need to route the ip
````
  ./docker.sh ip-route
````

Login to the hadoop-master or any container. The password is 'neverwinterdp'
````
  ssh neverwinterdp@hadoop-master
````

Go to /opt directory, you should have the cluster.sh script , hadoop, zookeeper, kafka installed here.
````
  cd /opt
````

Launch zookeeper, kafka and hadoop
````
  ./cluster.sh start --clean
````


Build and run Scribengin
````
 cd path-to/Scribengin/V2
 gradle clean build install release -x test
 ./build/release/Scribengin.V2/bin/scribengin.sh
````
