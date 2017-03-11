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
package akkeeper.launcher.yarn

import java.io.ByteArrayInputStream
import java.util
import java.util.concurrent.Executors

import akka.actor.Address
import akkeeper.launcher._
import akkeeper.master.MasterMain
import akkeeper.utils.CliArguments._
import akkeeper.utils.ConfigUtils._
import akkeeper.utils.yarn._
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}


private[akkeeper] class YarnLauncher(yarnConf: YarnConfiguration) extends Launcher {
  private val logger = LoggerFactory.getLogger(classOf[YarnLauncher])

  private val yarnClient = YarnClient.createYarnClient()
  yarnClient.init(yarnConf)

  private val pollingStatusExecutor = Executors.newSingleThreadExecutor()
  private implicit val pollingStatusExecutionContext = ExecutionContext
    .fromExecutor(pollingStatusExecutor)

  private def retrieveMasterAddress(config: Config,
                                    appId: ApplicationId,
                                    pollInterval: Long): Future[Address] = {
    @tailrec
    def retry(): Address = {
      val appReport = yarnClient.getApplicationReport(appId)
      val state = appReport.getYarnApplicationState
      state match {
        case s @ (YarnApplicationState.SUBMITTED | YarnApplicationState.ACCEPTED) =>
          logger.debug(s"Akkeeper Master is $s")
          Thread.sleep(pollInterval)
          retry()
        case YarnApplicationState.RUNNING =>
          val addr = Address("akka.tcp", config.getActorSystemName, appReport.getHost,
            config.getInt("akkeeper.akka.port"))
          logger.info(s"Akkeeper Master address is $addr")
          addr
        case other =>
          throw new YarnLauncherException(s"Unexpected Akkeeper Master state: $other")
      }
    }
    Future {
      retry()
    }
  }

  private def getResource(config: Config): Resource = {
    val yarnConfig = config.getYarnConfig.getConfig("master")
    val cpus = yarnConfig.getInt("cpus")
    val memory = yarnConfig.getInt("memory")
    Resource.newInstance(memory, cpus)
  }

  private def buildLocalResources(appId: String,
                                  config: Config,
                                  args: LaunchArguments): util.HashMap[String, LocalResource] = {
    val stagingDir = config.getYarnStagingDirectory(yarnConf, appId)
    val resourceManger = new YarnLocalResourceManager(yarnConf, stagingDir)
    val localResources = new util.HashMap[String, LocalResource]()

    val akkeeperJar = resourceManger.createLocalResource(args.akkeeperJarPath.getAbsolutePath,
      LocalResourceNames.AkkeeperJarName)
    localResources.put(LocalResourceNames.AkkeeperJarName, akkeeperJar)

    // Add a user jar to the staging directory. No need to include it
    // into master local resources.
    resourceManger.uploadLocalResource(args.userJar.getAbsolutePath,
      LocalResourceNames.UserJarName)

    args.otherJars.foreach(jarFile => {
      // Just upload the third-party jars. No need to include them
      // into master local resources.
      val localPath = LocalResourceNames.ExtraJarsDirName + "/" + jarFile.getName
      resourceManger.uploadLocalResource(jarFile.getAbsolutePath, localPath)
    })

    args.resources.foreach(resource => {
      // Distribute resources.
      val localPath = LocalResourceNames.ResourcesDirName + "/" + resource.getName
      resourceManger.uploadLocalResource(resource.getAbsolutePath, localPath)
    })

    args.principal.foreach(_ => {
      // Distribute keytab.
      val keytabResource = resourceManger.createLocalResource(args.keytab.getAbsolutePath,
        LocalResourceNames.KeytabName)
      localResources.put(LocalResourceNames.KeytabName, keytabResource)
    })

    args.userConfig.foreach(config => {
      val userConfigString = config.root().render(ConfigRenderOptions.concise())
      val configResource = resourceManger.createLocalResource(
        new ByteArrayInputStream(userConfigString.getBytes("UTF-8")),
        LocalResourceNames.UserConfigName)
      localResources.put(LocalResourceNames.UserConfigName, configResource)
    })
    localResources
  }

  private def buildCmd(appId: ApplicationId, config: Config,
                       args: LaunchArguments): List[String] = {
    val defaulJvmArgs = config.getYarnConfig.getStringList("master.jvm.args").asScala
    val jvmArgs = defaulJvmArgs ++ args.masterJvmArgs

    val userConfigArg = args.userConfig
      .map(_ => List(s"--$ConfigArg", LocalResourceNames.UserConfigName))
      .getOrElse(List.empty)
    val principalArg = args.principal
      .map(p => List(s"--$PrincipalArg", p))
      .getOrElse(List.empty)

    val appArgs = List(
      s"--$AppIdArg", appId.toString
    ) ++ userConfigArg ++ principalArg
    val mainClass = MasterMain.getClass.getName.replace("$", "")
    YarnUtils.buildCmd(mainClass, jvmArgs = jvmArgs, appArgs = appArgs)
  }

  override def start(): Unit = {
    yarnClient.start()
  }

  override def stop(): Unit = {
    yarnClient.stop()
    pollingStatusExecutor.shutdownNow()
  }

  override def launch(config: Config, args: LaunchArguments): Future[LaunchResult] = {
    val application = yarnClient.createApplication()

    val appContext = application.getApplicationSubmissionContext
    val appId = appContext.getApplicationId

    appContext.setKeepContainersAcrossApplicationAttempts(true)
    appContext.setApplicationName(config.getYarnApplicationName)

    val globalMaxAttempts = yarnConf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    val configMaxAttempts = config.getYarnConfig.getInt("max-attempts")
    val actualMaxAttempts = Math.min(globalMaxAttempts, configMaxAttempts)
    logger.debug(s"Akkeeper application maximum number of attempts is $actualMaxAttempts")
    appContext.setMaxAppAttempts(actualMaxAttempts)

    appContext.setResource(getResource(config))

    val localResources = buildLocalResources(appId.toString, config, args)
    val cmd = buildCmd(appId, config, args)
    logger.debug(s"Akkeeper Master command: ${cmd.mkString(" ")}")

    val amContainer = ContainerLaunchContext.newInstance(
      localResources, null, cmd.asJava, null, null, null)
    appContext.setAMContainerSpec(amContainer)

    yarnClient.submitApplication(appContext)
    logger.info(s"Launched Akkeeper Cluster $appId")

    val masterAddressFuture = retrieveMasterAddress(config, appId, args.pollInterval)
    masterAddressFuture.map(addr => LaunchResult(appId.toString, addr))
  }
}
