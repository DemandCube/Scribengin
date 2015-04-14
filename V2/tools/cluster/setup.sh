#Need build-essential package to build paramiko dependency
if [ -n "$(which apt-get)" ] ; then
  sudo apt-get install python-dev build-essential -y
fi

if [ -n "$(which yum)" ] ; then
  sudo yum groupinstall "Development tools" -y
  sudo yum install python-devel -y
fi

#Setup easy_install
wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python

#Install required modules
sudo easy_install nose==1.3.4 tabulate paramiko junit-xml click 
