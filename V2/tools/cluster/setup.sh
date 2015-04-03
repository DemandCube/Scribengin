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

#The previous step runs in the background on some machines, so loop until its done
while [ "`which easy_install`" == "" ] ; do
  sleep 1
done

#Install required modules
sudo easy_install nose==1.3.4 tabulate paramiko junit-xml click

echo "Setup complete!"