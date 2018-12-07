# StandaloneBundler
Standalone version of the bundler application.


## Pre-requisites
* Java (1.8 or higher) 
* git (v1.7 or higher)
* Maven (v3.3.8 or higher)

## Download the Source and Build the EAR File
* Download source
```
# cd /var/local/src
# git clone https://github.com/carpenlc/StandaloneBundler.git
```
* Install the Oracle JDBC drivers into the local maven repository.
```
# cd Replication-on-Demand
# mvn install:install-file -Dfile=./lib/ojdbc7.jar -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.1.0 -Dpackaging=jar
```
* Execute the Maven targets to build the output EAR
```
# cd /var/local/src/StandaloneBundler
# mvn clean package 
```
* The deployable EAR file will reside at the following location
```
/var/local/src/StandaloneBundler/target/bundler.war


