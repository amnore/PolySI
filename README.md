## Build

PolySI is tested on Ubuntu 22.04 LTS. Follow the instructions below to build it.

```sh
sudo apt update
sudo apt install g++ openjdk-11-jdk cmake libgmp-dev zlib1g-dev
git clone --recurse-submodules https://github.com/amnore/PolySI
cd PolySI
./gradlew jar
java -jar build/libs/CobraVerifier-0.0.1-SNAPSHOT.jar
```
