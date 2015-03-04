#!/usr/bin/env bash


function h1() {
	echo "****************************************************************************************"
	echo "$1"
	echo "****************************************************************************************"
}

h1 "Create and configure the neverwinterdp account"

useradd -m -d /home/neverwinterdp -s /bin/bash -c "neverwinterdp user" -p $(openssl passwd -1 neverwinterdp)  neverwinterdp
echo "neverwinterdp ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

cp -r /tmp/post-install/ssh-config /home/neverwinterdp/.ssh
chown -R neverwinterdp:neverwinterdp /home/neverwinterdp/.ssh

h1 "Patch the hadoop, zookeeper and kafka configuration"

cp -r /tmp/post-install/cluster.sh /opt

cp -r /tmp/post-install/zookeeper/* /opt/zookeeper/

cp -r /tmp/post-install/kafka/*   /opt/kafka/

cp -r /tmp/post-install/hadoop/*  /opt/hadoop/

h1 "Set up ssh for neverwinterdp user"
sudo cat /tmp/post-install/ssh-config/id_rsa.pub > /home/neverwinterdp/.ssh/authorized_keys
chmod -R 700 /home/neverwinterdp/.ssh/ && chmod 644 /home/neverwinterdp/.ssh/authorized_keys

h1 "Change /opt ownership and permission to neverwinterdp user"
chown -R neverwinterdp:neverwinterdp /opt
