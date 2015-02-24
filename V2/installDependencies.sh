git clone https://github.com/DemandCube/Queuengin/
git clone https://github.com/DemandCube/NeverwinterDP-Commons
cd Queuengin
gradle clean build install -x test
cd ../NeverwinterDP-Commons
gradle clean build install -x test
cd ../