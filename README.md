# Akkeeper

[![Build Status](https://travis-ci.org/izeigerman/akkeeper.svg?branch=master)](https://travis-ci.org/izeigerman/akkeeper)
[![Coverage Status](https://coveralls.io/repos/github/izeigerman/akkeeper/badge.svg?branch=master)](https://coveralls.io/github/izeigerman/akkeeper?branch=master)
[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

Akkeeper (Akka Keeper or Actor Kernel Keeper) - is an easy way to deploy your Akka application to a distributed environment. [Akka](http://akka.io/) is a widely used Actor framework, but there are still no good practices and approaches of deploying applications that are based on this framework. Akkeeper provides a powerful set of capabilities to maintain your cluster. You can easily deploy, terminate and monitor your services at runtime. Akkeeper was built keeping Hadoop as a primary use case, that's why it currently supports only [YARN](https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/YARN.html) as a resource manager. But this doesn't mean that other environments won't appear in future. Apache Spark and Apache Flink are good examples of Akka applications on Hadoop. Although both of them are data processing frameworks, I realised that YARN is not only MapReduce and can be used to distribute any kind of application. As a result your application acquires elasticity and resilience out of the box.

Some of the features provided by Akkeeper:
* Builds the Akka Cluster and automatically discovers all cluster participants.
* Allows to launch and terminate instances at runtime.
* Application Master fault tolerance. Rejoins the existing cluster after restart.

Here are several ways of how Akkeeper can be useful for your project:
* Distribute your microservices using Akkeeper.
* Keep your master service(s) separately and use Akkeeper to launch new workers/executors on demand.

The project documentation is under construction.

## How to build
Requirements:
```
Java 8
SBT version >= 1.0.0
```
To build a bundle for Scala 2.11: 
```
// ZIP
sbt ++2.11.11 universal:packageBin
// TGZ
sbt ++2.11.11 universal:packageZipTarball
```
Scala 2.12:
```
// ZIP
sbt ++2.12.6 universal:packageBin
// TGZ
sbt ++2.12.6 universal:packageZipTarball
```
To build examples:
```
sbt package
```

## Basic concepts
To understand how Akkeeper works you have to be aware of only two concepts: containers and instances.

### Container
Container defines an environment where the instance is running. Container determines how many resources should be allocated for the instance and what actors should be launched as part of this instance. Containers can be added, removed and modified dynamically at runtime.

### Instance
Instance is an execution unit in Akkeeper. Instance is just a running process with capabilities and properties of its container. "Deploy container" or "launch instance of container `container_name`" means the same - launching a process on some node in a cluster using the specified container's definition. Instances can be launched, monitored and terminated dynamically at runtime.

## How to use
Download and unpack the latest Akkeeper package:
- Scala 2.11: [ZIP](https://bintray.com/izeigerman/akkeeper/download_file?file_path=akkeeper_2.11-0.4.5.zip) / [TGZ](https://bintray.com/izeigerman/akkeeper/download_file?file_path=akkeeper_2.11-0.4.5.tgz)
- Scala 2.12: [ZIP](https://bintray.com/izeigerman/akkeeper/download_file?file_path=akkeeper_2.12-0.4.5.zip) / [TGZ](https://bintray.com/izeigerman/akkeeper/download_file?file_path=akkeeper_2.12-0.4.5.tgz)

In order to use the Scala API the following dependency must be added to `build.sbt`:
```
libraryDependencies += "com.github.izeigerman" %% "akkeeper-api" % "0.4.5"
```
In case if you need to launch Akkeeper from your code the following dependency must be introduced:
```
libraryDependencies += "com.github.izeigerman" %% "akkeeper-launcher" % "0.4.5"
```
The easiest way to start using Akkeeper is through the configuration file. Here is a quick start configuration file example:
```
akkeeper {
  # The list of container definitions.
  containers = [
    {
      # The unique name of the container.
      name = "myContainer"
      # The list of actors that will be launched in scope of this container.
      actors = [
        {
          # The actor's name.
          name = "myActor"
          # The fully qualified name of the Actor implementation.
          fqn = "com.test.MyActor"
        }
      ]
      # The number of CPUs that has to be allocated.
      cpus = 1
      # The amount of RAM in MB that has to be allocated.
      memory = 1024
      # Additional JVM arguments that will be passed to instances of this container.
      jvm-args = [ "-Xmx2G" ]
      # Custom Java properties. Can be used to override the configuration values.
      properties {
        myapp.myservice.property = "value"
        akka.cluster.roles.0 = "myRole1"
        akka.cluster.roles.1 = "myRole2"
      }
    }
  ]
}
```
Make sure your `HADOOP_CONF_DIR` and `YARN_CONF_DIR` environment variables point to the directory where the Hadoop configuration files are stored. Also `ZK_QUORUM` variable should contain a comma-separated list of ZooKeeper servers.
Now just pass this file together with your JAR archive which contains actor `com.test.MyActor` to Akkeeper:
```
./bin/akkeeper-submit --config ./config.conf /path/to/my.jar
```

## How to deploy and monitor instances at runtime

### REST API
[REST API documentation](https://github.com/akkeeper-project/akkeeper/blob/master/docs/rest.md)

### Akkeeper Actor services
This approach is applicable only when you're managing an Akkeeper cluster from the application or service which is part of the same Akka cluster.

#### Deploy API
Use this API to launch new instances on a cluster.
```scala
import akkeeper.api._
import akkeeper.master.service.DeployService
...
val actorSystem = ActorSystem("AkkeeperSystem")
// Create a remote Deploy Service actor reference.
val deployService = DeployService.createRemote(actorSystem)
// Launch 1 instance of container "myContainer".
(deployService ? DeployContainer("myContainer", 1)).onSuccess {
  case SubmittedInstances(requestId, containerName, instanceIds) => // submitted successfully.
  case OperationFailed(requestId, reason) => // deploy failed.
}
```

#### Monitoring API
Use this API to track instance status or terminate a running instance.
```scala
import akkeeper.api._
import akkeeper.master.service.MonitoringService
...
val actorSystem = ActorSystem("AkkeeperSystem")
// Create a remote Monitoring Service actor reference.
val monitoringService = MonitoringService.createRemote(actorSystem)
// Fetch the list of running instances.
(monitoringService ? GetInstances()).onSuccess {
  case InstancesList(requestId, instanceIds) => // a list of running instances.
  case OperationFailed(requestId, reason) => // fetching process failed.
}
```
[Here](https://github.com/akkeeper-project/akkeeper/blob/master/akkeeper-api/src/main/scala/akkeeper/api/MonitoringApi.scala) is the list of all messages supported by the Monitoring Service.

#### Container API
Use this API to fetch, create, update or delete container definitions.
```scala
import akkeeper.api._
import akkeeper.master.service.ContainerService
...
val actorSystem = ActorSystem("AkkeeperSystem")
// Create a remote Container Service actor reference.
val containerService = ContainerService.createRemote(actorSystem)
// Fetch the list of existing containers.
(containerService ? GetContainers()).onSuccess {
  case ContainersList(requestId, containerNames) => // a list of available containers.
  case OperationFailed(requestId, reason) => // fetching process failed.
}
```
[Here](https://github.com/akkeeper-project/akkeeper/blob/master/akkeeper-api/src/main/scala/akkeeper/api/ContainerApi.scala) is the list of all messages supported by the Container Service.

