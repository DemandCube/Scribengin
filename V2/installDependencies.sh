git clone https://github.com/DemandCube/Queuengin/
git clone https://github.com/DemandCube/NeverwinterDP-Commons
cd Queuengin
git pull
../../gradlew clean build install -x test
cd ../NeverwinterDP-Commons
git pull
../../gradlew clean build install -x test
cd ../
cd jvmagent/registry
git pull
../../../gradlew clean build release
cd ../../
../gradlew eclipse