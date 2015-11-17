# MapR-DB & OJAI Tests

This project contains tests for OJAI, the JSON API For MapR-DB.


### Pre-requisites

* Java SDK 7 or newer
* Maven 3
* MapR-DB JSON Developer Preview VM

In your MapR-DB environment change the permissions of the `apps` folder

```
ssh mapr@maprdemo
 
cd /mapr/demo.mapr.com/

chmod 777 apps
```


## Usage

Clone the repository, then

```
mvn clean package
```

and run the application using:

```
mvn exec:java -Dexec.mainClass="com.mapr.db.tests.TestJSONTablesCRUDOperations"
```


