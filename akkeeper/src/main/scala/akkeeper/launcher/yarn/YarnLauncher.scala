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

import java.util

import akkeeper.launcher.{Launcher, LaunchArguments}
import akkeeper.master.MasterMain
import akkeeper.utils.CliArguments._
import akkeeper.utils.ConfigUtils._
import akkeeper.utils.yarn._
import com.typesafe.config.Config
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._


private[akkeeper] class YarnLauncher(yarnConf: YarnConfiguration) extends Launcher {
  private val logger = LoggerFactory.getLogger(classOf[YarnLauncher])

  private val yarnClient = YarnClient.createYarnClient()
  yarnClient.init(yarnConf)

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
    resourceManger.createLocalResource(args.userJar.getAbsolutePath,
      LocalResourceNames.UserJarName)

    args.otherJars.foreach(jarFile => {
      // Just upload the third-party jars. No need to include them
      // into master local resources.
      val localPath = LocalResourceNames.ExtraJarsDirName + "/" + jarFile.getName
      resourceManger.createLocalResource(jarFile.getAbsolutePath, localPath)
    })

    args.resources.foreach(resource => {
      // Distribute resources.
      val localPath = LocalResourceNames.ResourcesDirName + "/" + resource.getName
      resourceManger.createLocalResource(resource.getAbsolutePath, localPath)
    })

    args.config.foreach(config => {
      val configResource = resourceManger.createLocalResource(config.getAbsolutePath,
        LocalResourceNames.ConfigName)
      localResources.put(LocalResourceNames.ConfigName, configResource)
    })
    localResources
  }

  private def buildCmd(appId: ApplicationId, config: Config,
                       args: LaunchArguments): List[String] = {
    val defaulJvmArgs = config.getYarnConfig.getStringList("master.jvm.args").asScala
    val jvmArgs = defaulJvmArgs ++ args.masterJvmArgs
    val appArgs = List(
      s"--$AppIdArg", appId.toString
    ) ++ args.config
      .map(_ => List(s"--$ConfigArg", LocalResourceNames.ConfigName))
      .getOrElse(List.empty)
    val mainClass = MasterMain.getClass.getName.replace("$", "")
    YarnUtils.buildCmd(mainClass, jvmArgs = jvmArgs, appArgs = appArgs)
  }

  def start(): Unit = {
    yarnClient.start()
  }

  def stop(): Unit = {
    yarnClient.stop()
  }

  def launch(config: Config, args: LaunchArguments): String = {
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
      localResources, sys.env.asJava, cmd.asJava, null, null, null)
    appContext.setAMContainerSpec(amContainer)

    yarnClient.submitApplication(appContext)
    logger.info(s"Launched Akkeeper Cluster $appId")
    appId.toString
  }
}
