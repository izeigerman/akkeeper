/*
 * Copyright 2017 Iaroslav Zeigerman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package akkeeper.deploy.yarn

import java.io.{FileNotFoundException, ByteArrayInputStream}
import java.util
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}

import akkeeper.common.{ContainerDefinition, InstanceId}
import akkeeper.container.ContainerInstanceMain
import akkeeper.deploy._
import akkeeper.utils.CliArguments._
import akkeeper.utils.ConfigUtils._
import akkeeper.utils.yarn._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util._
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import YarnApplicationMaster._

private[akkeeper] class YarnApplicationMaster(config: YarnApplicationMasterConfig,
                                              yarnClient: YarnMasterClient)
  extends DeployClient.Async {

  private val logger = LoggerFactory.getLogger(classOf[YarnApplicationMaster])

  private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(
    config.config.getInt("akkeeper.yarn.client-threads"))

  private val stagingDirectory: String = config.config
    .getYarnStagingDirectory(config.yarnConf, config.appId)
  private val localResourceManager: YarnLocalResourceManager =
    new YarnLocalResourceManager(config.yarnConf, stagingDirectory)
  private val instanceCommonResources: Map[String, LocalResource] = buildInstanceCommonResources

  private var priorityCounter: Int = 0
  private val pendingResults: mutable.Map[InstanceId, Promise[DeployResult]] =
    mutable.Map.empty
  private val containerToInstance: mutable.Map[ContainerId, InstanceId] =
    mutable.Map.empty
  private val pendingInstances: mutable.Map[Int, (ContainerDefinition, InstanceId)] =
    mutable.Map.empty

  private var isRunning: Boolean = false

  private def buildContainerRequest(container: ContainerDefinition): ContainerRequest = {
    val priority = Priority.newInstance(priorityCounter)
    priorityCounter = (priorityCounter + 1) % MaxPriority
    val capability = Resource.newInstance(container.memory, container.cpus)
    new ContainerRequest(capability, null, null, priority)
  }

  private def buildInstanceCommonResources: Map[String, LocalResource] = {
    val localResources = mutable.Map.empty[String, LocalResource]

    // Distribute the user configuration.
    try {
      val instanceConfigResource = localResourceManager.getExistingLocalResource(
        LocalResourceNames.UserConfigName)
      localResources.put(LocalResourceNames.UserConfigName, instanceConfigResource)
    } catch  {
      case _: FileNotFoundException =>
        logger.debug("No user configuration was found")
    }

    // Retrieve the Akkeeper Assembly jar.
    val akkeeperJarResource = localResourceManager
      .getExistingLocalResource(LocalResourceNames.AkkeeperJarName)
    localResources.put(LocalResourceNames.AkkeeperJarName, akkeeperJarResource)

    // Retrieve the user jar.
    val userJarResource = localResourceManager
      .getExistingLocalResource(LocalResourceNames.UserJarName)
    localResources.put(LocalResourceNames.UserJarName, userJarResource)

    val fs = FileSystem.get(config.yarnConf)
    def addExistingResources(directory: String): Unit = {
      try {
        val resources = fs.listStatus(new Path(stagingDirectory, directory))
        resources.foreach(status => {
          val fileName = directory + "/" + status.getPath.getName
          val resource = localResourceManager.getExistingLocalResource(fileName)
          localResources.put(fileName, resource)
        })
      } catch {
        case _: FileNotFoundException =>
      }
    }
    // Retrieve a content of the jars/ directory.
    addExistingResources(LocalResourceNames.ExtraJarsDirName)
    // Retrieve a content of the resources/ directory.
    addExistingResources(LocalResourceNames.ResourcesDirName)

    localResources.toMap
  }

  private def buildActorLaunchContextResource(containerDefinition: ContainerDefinition,
                                              instanceId: InstanceId): LocalResource = {
    import spray.json._
    import akkeeper.common.ContainerDefinitionJsonProtocol._
    val jsonStr = containerDefinition.actors.toJson.compactPrint
    localResourceManager.createLocalResource(new ByteArrayInputStream(jsonStr.getBytes("UTF-8")),
      s"actors_$instanceId.json")
  }

  private def launchInstance(container: Container,
                             containerDefinition: ContainerDefinition,
                             instanceId: InstanceId): Unit = {
    val actorLaunchResource = buildActorLaunchContextResource(containerDefinition, instanceId)
    val instanceResources = instanceCommonResources + (
      LocalResourceNames.ActorLaunchContextsName -> actorLaunchResource
    )

    val env = containerDefinition.environment
    val javaArgs = containerDefinition.jvmArgs ++ containerDefinition.jvmProperties.map {
      case (name, value) => s"-D$name=$value"
    }
    val mainClass = ContainerInstanceMain.getClass.getName.replace("$", "")
    val appArgs = List(
      s"--$InstanceIdArg", instanceId.toString,
      s"--$AppIdArg", config.appId,
      s"--$MasterAddressArg", config.selfAddress.toString,
      s"--$ActorLaunchContextsArg", LocalResourceNames.ActorLaunchContextsName
    ) ++ instanceResources.get(LocalResourceNames.UserConfigName)
      .map(_ => List(s"--$ConfigArg", LocalResourceNames.UserConfigName))
      .getOrElse(List.empty)

    val extraClassPath = List(
      LocalResourceNames.UserJarName,
      LocalResourceNames.ExtraJarsDirName + "/*"
    )

    val cmd = YarnUtils.buildCmd(mainClass, extraClassPath, javaArgs, appArgs)
    logger.debug(s"Instance $instanceId command: ${cmd.mkString(" ")}")

    val launchContext = ContainerLaunchContext.newInstance(
      instanceResources.asJava, env.asJava, cmd.asJava,
      null, null, null)

    Try(yarnClient.startContainer(container, launchContext)) match {
      case Success(_) => onContainerStarted(container.getId)
      case Failure(e) => onStartContainerError(container.getId, e)
    }
  }

  private def onContainerLaunchResult(containerId: ContainerId, result: DeployResult): Unit = {
    val instanceId = result.instanceId
    pendingResults(instanceId) success result

    pendingResults.remove(instanceId)
    containerToInstance.remove(containerId)
  }

  private def onContainerStarted(containerId: ContainerId): Unit = synchronized {
    val instanceId = containerToInstance(containerId)
    logger.debug(s"Container $containerId (instance $instanceId) successfully started")
    onContainerLaunchResult(containerId, DeploySuccessful(instanceId))
  }

  private def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = synchronized {
    val instanceId = containerToInstance(containerId)
    logger.error(s"Failed to launch container $containerId (instance $instanceId)", t)
    onContainerLaunchResult(containerId, DeployFailed(instanceId, t))
    yarnClient.releaseAssignedContainer(containerId)
  }

  private def onContainersAllocated(containers: util.List[Container]): Unit = synchronized {
    for (container <- containers.asScala) {
      val priority = container.getPriority.getPriority
      if (pendingInstances.contains(priority)) {
        val (containerDef, instanceId) = pendingInstances(priority)
        logger.debug(s"Launching container ${container.getId} for instance $instanceId")
        containerToInstance.put(container.getId, instanceId)
        executorService.submit(new Runnable {
          override def run(): Unit = launchInstance(container, containerDef, instanceId)
        })
      } else {
        logger.debug(s"Unknown container allocation ${container.getId}. Realesing container")
        yarnClient.releaseAssignedContainer(container.getId)
      }
    }
  }

  override def deploy(container: ContainerDefinition,
                      instances: Seq[InstanceId]): Seq[Future[DeployResult]] = synchronized {
    instances.map(id => {
      logger.debug(s"Deploying instance $id (container ${container.name})")

      val promise = Promise[DeployResult]()
      pendingResults.put(id, promise)

      val request = buildContainerRequest(container)
      pendingInstances.put(request.getPriority.getPriority, container -> id)

      yarnClient.addContainerRequest(request)
      promise.future
    })
  }

  private def allocateResources(): Unit = {
    val allocateResponse = yarnClient.allocate(0.2f)
    onContainersAllocated(allocateResponse.getAllocatedContainers)
  }

  private def scheduleAllocateResources(): Unit = {
    executorService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = allocateResources()
    }, AMHeartbeatInterval, AMHeartbeatInterval, TimeUnit.MILLISECONDS)
  }

  override def start(): Unit = {
    yarnClient.init(config.yarnConf)
    yarnClient.start()

    val localAddr = config.selfAddress.host
      .getOrElse(throw YarnMasterException("The self address is not specified"))
    yarnClient.registerApplicationMaster(localAddr, 0, config.trackingUrl)

    scheduleAllocateResources()

    isRunning = true
    logger.info("YARN Application Master started")
  }

  private def unregisterApplicationMaster(status: FinalApplicationStatus, message: String): Unit = {
    try {
      yarnClient.unregisterApplicationMaster(status, message, "")
    } catch {
      case NonFatal(e) =>
        logger.error("Failed to unregister application", e)
    }
  }

  private def stopClients(): Unit = {
    yarnClient.stop()
    executorService.shutdown()
    isRunning = false
  }

  override def stop(): Unit = {
    if (isRunning) {
      unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "")
      stopClients()
      logger.info("YARN Application Master stopped")
    }
  }

  override def stopWithError(error: Throwable): Unit = {
    if (isRunning) {
      unregisterApplicationMaster(FinalApplicationStatus.FAILED, error.getMessage)
      stopClients()
      logger.error("YARN Application Master stopped with errors", error)
    }
  }
}

object YarnApplicationMaster {
  private[yarn] val AMHeartbeatInterval = 1000
  private val MaxPriority: Int = 32767
}
