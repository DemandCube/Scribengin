#!/usr/bin/env bash


function printHeader() {
	echo "****************************************************************************************"
	echo "$1"
	echo "****************************************************************************************"
}

printHeader "Run the post installation script"

useradd -m -d /home/neverwinterdp -s /bin/bash -c "neverwinterdp user" -p $(openssl passwd -1 neverwinterdp)  neverwinterdp
echo "neverwinterdp ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

cp -r /opt/ssh-config /home/neverwinterdp/.ssh
chown -R neverwinterdp:neverwinterdp /home/neverwinterdp/.ssh


echo "Change /opt ownership and permission to neverwinterdp user"
chown -R neverwinterdp:neverwinterdp /opt
