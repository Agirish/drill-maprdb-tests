# Drill & MapR-DB Tests

This project contains Tests & Utils for Drill Integration with MapR-DB Document Database (JSON Tables).

### Pre-requisites

* Java SDK 7 or newer
* Maven 3
* MapR-DB JSON Installation

## Usage

Clone the repository, then

```
mvn clean package
```

Sample run command:

```
mvn exec:java -Dexec.mainClass="com.mapr.db.tests.CreateJSONTable"
```
