/*
 * Copyright 2017-2018 Iaroslav Zeigerman
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
import java.net.URI
import java.util

import akkeeper.common.CliArguments._
import akkeeper.common.config._
import akkeeper.launcher._
import akkeeper.yarn._
import akkeeper.yarn.client.YarnLauncherClient
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}


final class YarnLauncher(yarnConf: YarnConfiguration,
                         yarnClientCreator: () => YarnLauncherClient)
                        (implicit context: ExecutionContext) extends Launcher {

  private val logger = LoggerFactory.getLogger(classOf[YarnLauncher])

  private def withYarnClient[T](f: YarnLauncherClient => Future[T]): Future[T] = {
    val yarnClient = yarnClientCreator()
    val yarnClientStarted = Future {
      yarnClient.init(yarnConf)
      yarnClient.start()
    }
    val result = yarnClientStarted.flatMap(_ => f(yarnClient))
    result.andThen { case _ => yarnClient.stop() }
  }

  private def retrieveMasterHost(yarnClient: YarnLauncherClient,
                                 config: Config,
                                 appId: ApplicationId,
                                 pollInterval: Long): Future[String] = {
    @tailrec
    def retry(): String = {
      val appReport = yarnClient.getApplicationReport(appId)
      val state = appReport.getYarnApplicationState
      state match {
        case s @ (YarnApplicationState.SUBMITTED | YarnApplicationState.ACCEPTED) =>
          logger.debug(s"Akkeeper Master is $s")
          Thread.sleep(pollInterval)
          retry()
        case YarnApplicationState.RUNNING =>
          logger.info(s"Akkeeper Master host is ${appReport.getHost}")
          appReport.getHost
        case other =>
          throw new YarnLauncherException(s"Unexpected Akkeeper Master state: $other\n" +
            appReport.getDiagnostics)
      }
    }
    Future {
      retry()
    }
  }

  private def getResource(config: Config): Resource = {
    val cpus = config.yarn.masterCpus
    val memory = config.yarn.masterMemory
    Resource.newInstance(memory, cpus)
  }

  private def uploadGenericFiles(files: Seq[URI], dstLocalDir: String,
                                 resourceManger: YarnLocalResourceManager): Unit = {
    files.foreach(file => {
      val fileName = FilenameUtils.getName(file.getPath)
      val localPath = dstLocalDir + "/" + fileName
      resourceManger.uploadLocalResource(file.toString, localPath)
    })
  }

  private def buildLocalResources(stagingDir: String,
                                  args: LaunchArguments,
                                  launcherConfig: Config): util.HashMap[String, LocalResource] = {
    val resourceManager = new YarnLocalResourceManager(yarnConf, stagingDir)
    val localResources = new util.HashMap[String, LocalResource]()

    def uploadConfig(localResourceName: String, config: Config): Unit = {
      val userConfigString = config.root().render(ConfigRenderOptions.concise())
      val configResource = resourceManager.uploadLocalResource(
        new ByteArrayInputStream(userConfigString.getBytes("UTF-8")), localResourceName)
      localResources.put(localResourceName, configResource)
    }

    val akkeeperJar = resourceManager.uploadLocalResource(args.akkeeperJarPath.toString,
      LocalResourceNames.AkkeeperJarName)
    localResources.put(LocalResourceNames.AkkeeperJarName, akkeeperJar)

    // Add a user jar to the staging directory. No need to include it
    // into master local resources.
    resourceManager.uploadLocalResource(args.userJar.toString,
      LocalResourceNames.UserJarName)

    // Just upload the third-party jars. No need to include them
    // into master local resources.
    uploadGenericFiles(args.otherJars, LocalResourceNames.ExtraJarsDirName, resourceManager)

    // Distribute resources.
    uploadGenericFiles(args.resources, LocalResourceNames.ResourcesDirName, resourceManager)

    args.principal.foreach(_ => {
      // Distribute keytab.
      val keytabResource = resourceManager.uploadLocalResource(args.keytab.get.toString,
        LocalResourceNames.KeytabName)
      localResources.put(LocalResourceNames.KeytabName, keytabResource)
    })

    args.userConfig.foreach(uploadConfig(LocalResourceNames.UserConfigName, _))
    uploadConfig(LocalResourceNames.LauncherConfigName, launcherConfig)

    localResources
  }

  private def buildCmd(appId: ApplicationId, config: Config,
                       args: LaunchArguments): List[String] = {
    val defaulJvmArgs = config.yarn.masterJvmArgs
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
    val mainClass = "akkeeper.master.MasterMain"
    YarnUtils.buildCmd(mainClass, jvmArgs = jvmArgs, appArgs = appArgs)
  }

  private def buildLauncherConfig(stagingDir: String, resources: Seq[AkkeeperResource]): Config = {
    val globalResourcesConfigValue = resources.map { r =>
      Map("uri" -> r.uri.toString, "local-path" -> r.localPath, "archive" -> r.archive).asJava
    }
    ConfigFactory.parseMap(
      Map(
        "akkeeper.yarn.staging-directory" -> stagingDir,
        "akkeeper.global-resources" -> globalResourcesConfigValue.asJava
      ).asJava
    )
  }

  private def launchWithClient(yarnClient: YarnLauncherClient,
                               config: Config,
                               args: LaunchArguments): Future[LaunchResult] = {
    val application = yarnClient.createApplication()

    val appContext = application.getApplicationSubmissionContext
    val appId = appContext.getApplicationId

    appContext.setKeepContainersAcrossApplicationAttempts(true)
    appContext.setApplicationName(config.yarn.applicationName)

    val globalMaxAttempts = yarnConf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    val configMaxAttempts = config.yarn.maxAttempts
    val actualMaxAttempts = Math.min(globalMaxAttempts, configMaxAttempts)
    logger.debug(s"Akkeeper application maximum number of attempts is $actualMaxAttempts")
    appContext.setMaxAppAttempts(actualMaxAttempts)

    appContext.setResource(getResource(config))

    val baseStagingDir = config.yarn.stagingDirectory.getOrElse(YarnUtils.defaultStagingDirectory(yarnConf))
    val stagingDir = YarnUtils.appStagingDirectory(yarnConf, Some(baseStagingDir), appId.toString)
    val launcherConfig = buildLauncherConfig(baseStagingDir, args.globalResources)
    val localResources = buildLocalResources(stagingDir, args, launcherConfig)
    val cmd = buildCmd(appId, config, args)
    logger.debug(s"Akkeeper Master command: ${cmd.mkString(" ")}")

    val tokens = args.principal
      .map(_ => YarnUtils.obtainContainerTokens(stagingDir, yarnConf))
      .orNull

    val amContainer = ContainerLaunchContext.newInstance(
      localResources, null, cmd.asJava, null, tokens, null)
    appContext.setAMContainerSpec(amContainer)

    args.yarnQueue.foreach(appContext.setQueue)

    yarnClient.submitApplication(appContext)
    logger.info(s"Launched Akkeeper Cluster $appId")

    val masterHostFuture = retrieveMasterHost(yarnClient, config, appId, args.pollInterval)
    masterHostFuture.map(LaunchResult(appId.toString, _))
  }

  override def launch(config: Config, args: LaunchArguments): Future[LaunchResult] = {
    withYarnClient { yarnClient =>
      launchWithClient(yarnClient, config, args)
    }
  }
}
