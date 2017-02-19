# Akkeeper

Akkeeper (Akka Keeper or Actor Kernel Keeper) - is an easy way to deploy your Akka application to a distributed environment. [Akka](http://akka.io/) is a widely used Actor framework, but there are still no good practices and approaches of deploying applications that are based on this framework. Akkeeper provides a powerful set of capabilities to maintain your cluster. You can easily deploy, terminate and monitor your services at runtime. Akkeeper was built keeping Hadoop as a primary use case, that's why it currently supports only [YARN](https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/YARN.html) as a resource manager. But this doesn't mean that other environments won't appear in future. Apache Spark and Apache Flink are good examples of Akka applications on Hadoop. Although both of them are data processing frameworks, I realised that YARN can be used to distribute any kind of application. As a result your application acquires elasticity and resilience out of the box. The idea of this project was heavily inspired by a similar solution for Java services called [BeansZoo](https://github.com/pelatimtt/beanszoo). Here are several ways of how Akkeeper can be useful for your project:
* Distribute your whole application using Akkeeper.
* Keep your master service(s) separately and use Akkeeper to launch new workers/executors on demand.

The project documentation is under construction.

## How to build
The required SBT version is >= 0.13.11
```
git clone https://github.com/izeigerman/akkeeper.git
cd akkeeper
sbt assembly
```

## How to use
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
      jvm {
        # Additional JVM arguments that will be passed to instances of this container.
        args = [ "-Xmx1G" ]
        # Additional JVM properties that will be passed to instances of this container.
        properties {
          property = "value"
        }
      }
    }
  ]
  # The list of instances that will be launched initially.
  instances = [
    {
      # The name of the container that should be launched.
      name = "myContainer"
      # The number of instances.
      quantity = 1
    }
  ]
}
```
Now just pass this file together with your JAR archive which contains actor `com.test.MyActor` to Akkeeper:
```
java -cp /path/to/akkeeper.jar akkeeper.launcher.LauncherMain --akkeeperJar /path/to/akkeeper.jar --config ./config.conf /path/to/my.jar
```
This is it, observe your running actor.

## How to deploy and monitor instances at runtime
### Using the Akkeeper Actor services
This approach is applicable only when you're managing an Akkeeper cluster from the application or service which is part of the same Akka cluster.
TBD

### Using REST
REST support is in progress.

