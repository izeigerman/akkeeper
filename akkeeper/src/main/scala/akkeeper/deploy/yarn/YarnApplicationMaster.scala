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
import java.nio.ByteBuffer
import java.util

import akkeeper.common.{ContainerDefinition, InstanceId}
import akkeeper.container.ContainerInstanceMain
import akkeeper.deploy._
import akkeeper.utils.CliArguments._
import akkeeper.utils.ConfigUtils._
import akkeeper.utils.yarn._
import com.typesafe.config._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import YarnApplicationMaster._

private[akkeeper] class YarnApplicationMaster(config: YarnApplicationMasterConfig)
  extends AMRMClientAsync.CallbackHandler
     with NMClientAsync.CallbackHandler
     with DeployClient.Async {

  private val logger = LoggerFactory.getLogger(classOf[YarnApplicationMaster])
  private val amrmClient =
    AMRMClientAsync.createAMRMClientAsync[ContainerRequest](AMHeartbeatInterval, this)
  private val nmClient = NMClientAsync.createNMClientAsync(this)
  private var yarnClusterResponse: Option[RegisterApplicationMasterResponse] = None

  private val stagingDirectory: String = config.config
    .getYarnStagingDirectory(config.yarnConf, config.appId)
  private val localResourceManager: YarnLocalResourceManager =
    new YarnLocalResourceManager(config.yarnConf, stagingDirectory)
  private val instanceCommonResources: Map[String, LocalResource] = buildInstanceCommonResources

  private val mastertLock = new Object()
  private val pendingResults: mutable.Map[InstanceId, Promise[DeployResult]] =
    mutable.Map.empty
  private val containerToInstance: mutable.Map[ContainerId, InstanceId] =
    mutable.Map.empty
  private val pendingInstances: mutable.Queue[(ContainerDefinition, InstanceId)] =
    mutable.Queue.empty

  private var isRunning: Boolean = false

  private def getClusterResponse: RegisterApplicationMasterResponse = {
    yarnClusterResponse
      .getOrElse(throw YarnMasterException("Yarn Application Master is not started"))
  }

  private def buildContainerRequest(container: ContainerDefinition): ContainerRequest = {
    val priority = Priority.UNDEFINED
    val capability = Resource.newInstance(container.memory, container.cpus)
    new ContainerRequest(capability, null, null, priority)
  }

  private def buildInstanceCommonResources: Map[String, LocalResource] = {
    val instanceConfig: ConfigObject = config.config.root()
    val instanceConfigString: String = instanceConfig.render(ConfigRenderOptions.concise())

    val localResources = mutable.Map.empty[String, LocalResource]

    // Distribute the configuration payload.
    val instanceConfigResource = localResourceManager.createLocalResource(
      new ByteArrayInputStream(instanceConfigString.getBytes("UTF-8")),
      LocalResourceNames.InstanceConfigName)
    localResources.put(LocalResourceNames.InstanceConfigName, instanceConfigResource)

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
      s"--$ConfigArg", LocalResourceNames.InstanceConfigName,
      s"--$ActorLaunchContextsArg", LocalResourceNames.ActorLaunchContextsName
    )
    val extraClassPath = List(
      LocalResourceNames.UserJarName,
      LocalResourceNames.ExtraJarsDirName + "/*"
    )

    val cmd = YarnUtils.buildCmd(mainClass, extraClassPath, javaArgs, appArgs)
    logger.debug(s"Instance $instanceId command: ${cmd.mkString(" ")}")

    val launchContext = ContainerLaunchContext.newInstance(
      instanceResources.asJava, env.asJava, cmd.asJava,
      null, null, null)

    nmClient.startContainerAsync(container, launchContext)
  }

  private def onContainerLaunchResult(containerId: ContainerId, result: DeployResult): Unit = {
    val instanceId = result.instanceId
    pendingResults(instanceId) success result

    pendingResults.remove(instanceId)
    containerToInstance.remove(containerId)
  }

  private def unregisterApplicationMaster(status: FinalApplicationStatus, message: String): Unit = {
    try {
      amrmClient.unregisterApplicationMaster(status, message, "")
    } catch {
      case NonFatal(e) =>
        logger.error("Failed to unregister application", e)
    }
  }

  private def stopClients(): Unit = {
    nmClient.stop()
    amrmClient.stop()
    isRunning = false
  }

  override def getProgress: Float = 0.2f

  override def onError(e: Throwable): Unit = {
    logger.error("Yarn Application Master unexpected error", e)
  }

  override def onShutdownRequest(): Unit = stop()

  override def onContainersAllocated(containers: util.List[Container]): Unit = synchronized {
    mastertLock.synchronized {
      for (container <- containers.asScala) {
        val (containerDef, instanceId) = pendingInstances.dequeue()
        logger.debug(s"Launching container ${container.getId} for instance $instanceId")
        containerToInstance.put(container.getId, instanceId)
        launchInstance(container, containerDef, instanceId)
      }
    }
  }

  override def onContainerStarted(containerId: ContainerId,
                                  allServiceResponse: util.Map[String, ByteBuffer]): Unit = {
    mastertLock.synchronized {
      val instanceId = containerToInstance(containerId)
      logger.debug(s"Container $containerId (instance $instanceId) successfully started")
      onContainerLaunchResult(containerId, DeploySuccessful(instanceId))
    }
  }

  override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = {
    mastertLock.synchronized {
      val instanceId = containerToInstance(containerId)
      logger.error(s"Failed to launch container $containerId (instance $instanceId)", t)
      onContainerLaunchResult(containerId, DeployFailed(instanceId, t))
    }
  }

  override def deploy(container: ContainerDefinition,
                      instances: Seq[InstanceId]): Seq[Future[DeployResult]] = {
    mastertLock.synchronized {
      instances.map(id => {
        logger.debug(s"Deploying instance $id (container ${container.name})")
        pendingInstances.enqueue(container -> id)

        val promise = Promise[DeployResult]()
        pendingResults.put(id, promise)

        val request = buildContainerRequest(container)
        amrmClient.addContainerRequest(request)

        promise.future
      })
    }
  }

  override def start(): Unit = {
    amrmClient.init(config.yarnConf)
    amrmClient.start()

    nmClient.init(config.yarnConf)
    nmClient.start()

    val localAddr = config.selfAddress.host
      .getOrElse(throw YarnMasterException("The self address is not specified"))
    yarnClusterResponse = Some(amrmClient.registerApplicationMaster(localAddr, 0,
      config.trackingUrl))
    isRunning = true
    logger.info("YARN Application Master started")
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

  // Methods that doesn't require implementation at this point.
  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {}
  override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = {}
  override def onContainerStatusReceived(containerId: ContainerId,
                                         containerStatus: ContainerStatus): Unit = {}
  override def onContainerStopped(containerId: ContainerId): Unit = {}
  override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = {}
  override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = {}
}

object YarnApplicationMaster {
  private[yarn] val AMHeartbeatInterval = 1000

  private val BlackListedConfigs = List(
    "akka", "awt", "file", "java", "line", "os", "path", "sun", "user"
  )
}
