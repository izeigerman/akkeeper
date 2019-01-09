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

import akka.actor.Address
import akkeeper.config._
import akkeeper.launcher._
import akkeeper.master.MasterMain
import akkeeper.utils.CliArguments._
import akkeeper.utils.yarn._
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}


final class YarnLauncher(yarnConf: YarnConfiguration,
                         yarnClientCreator: () => YarnLauncherClient)
                        (implicit context: ExecutionContext)extends Launcher[Future] {

  private val logger = LoggerFactory.getLogger(classOf[YarnLauncher])

  private def withYarnClient[T](f: YarnLauncherClient => T): T = {
    val yarnClient = yarnClientCreator()
    try {
      yarnClient.init(yarnConf)
      yarnClient.start()
      f(yarnClient)
    } finally {
      yarnClient.stop()
    }
  }

  private def retrieveMasterAddress(yarnClient: YarnLauncherClient,
                                    config: Config,
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
          val addr = Address("akka.tcp", config.akkeeperAkka.actorSystemName, appReport.getHost,
            config.akkeeperAkka.port)
          logger.info(s"Akkeeper Master address is $addr")
          addr
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
      resourceManger.createLocalResource(file.toString, localPath)
    })
  }

  private def buildLocalResources(stagingDir: String,
                                  args: LaunchArguments): util.HashMap[String, LocalResource] = {
    val resourceManger = new YarnLocalResourceManager(yarnConf, stagingDir)
    val localResources = new util.HashMap[String, LocalResource]()

    val akkeeperJar = resourceManger.createLocalResource(args.akkeeperJarPath.toString,
      LocalResourceNames.AkkeeperJarName)
    localResources.put(LocalResourceNames.AkkeeperJarName, akkeeperJar)

    // Add a user jar to the staging directory. No need to include it
    // into master local resources.
    resourceManger.createLocalResource(args.userJar.toString,
      LocalResourceNames.UserJarName)

    // Just upload the third-party jars. No need to include them
    // into master local resources.
    uploadGenericFiles(args.otherJars, LocalResourceNames.ExtraJarsDirName, resourceManger)

    // Distribute resources.
    uploadGenericFiles(args.resources, LocalResourceNames.ResourcesDirName, resourceManger)

    args.principal.foreach(_ => {
      // Distribute keytab.
      val keytabResource = resourceManger.createLocalResource(args.keytab.get.toString,
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
    val mainClass = MasterMain.getClass.getName.replace("$", "")
    YarnUtils.buildCmd(mainClass, jvmArgs = jvmArgs, appArgs = appArgs)
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

    val stagingDir = config.yarn.stagingDirectory(yarnConf, appId.toString)
    val localResources = buildLocalResources(stagingDir, args)
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

    val masterAddressFuture = retrieveMasterAddress(yarnClient, config, appId, args.pollInterval)
    masterAddressFuture.map(LaunchResult(appId.toString, _))
  }

  override def launch(config: Config, args: LaunchArguments): Future[LaunchResult] = {
    Future {
      withYarnClient { yarnClient =>
        launchWithClient(yarnClient, config, args)
      }
    }.flatMap(identity) // There is no Future.flatten method in Scala 2.11.
  }
}
