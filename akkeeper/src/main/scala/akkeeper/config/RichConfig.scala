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
package akkeeper.config

import java.time.{Duration => JavaDuration}
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import akkeeper.common.ContainerDefinition
import akkeeper.master.service.MasterService
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._

private[akkeeper] final class RichConfig(config: Config) {

  implicit def javaDuration2ScalaDuration(value: JavaDuration): FiniteDuration = {
    Duration.fromNanos(value.toNanos)
  }

  private def akkeeperConfig: Config = config.getConfig("akkeeper")

  // Top-level accessors
  private def akkaConfig: Config = akkeeperConfig.getConfig("akka")
  private def monitoringConfig: Config = akkeeperConfig.getConfig("monitoring")
  private def launcherConfig: Config = akkeeperConfig.getConfig("launcher")
  private def zookeeperConfig: Config = akkeeperConfig.getConfig("zookeeper")
  private def restConfig: Config = akkeeperConfig.getConfig("api.rest")
  private def kerberosConfig: Config = akkeeperConfig.getConfig("kerberos")
  private def yarnConfig: Config = akkeeperConfig.getConfig("yarn")
  private def yarnMaster: Config = yarnConfig.getConfig("master")

  // Akkeeper configs
  def containers: Seq[ContainerDefinition] = {
    if (akkeeperConfig.hasPath("containers")) {
      val configContainers = akkeeperConfig.getConfigList("containers").asScala
      configContainers.map(ContainerDefinition.fromConfig)
    } else {
      Seq.empty
    }
  }

  // Akka configs
  def akkaActorSystemName: String = akkaConfig.getString("system-name")
  def akkaPort: Int = akkaConfig.getInt("port")
  def akkaSeedNodesNum: Int = akkaConfig.getInt("seed-nodes-num")
  def akkaJoinClusterTimeout: FiniteDuration = akkaConfig.getDuration("join-cluster-timeout")
  def akkaLeaveClusterTimeout: FiniteDuration = akkaConfig.getDuration("leave-cluster-timeout")

  // Monitoring configs
  def monitoringLaunchTimeout: FiniteDuration = monitoringConfig.getDuration("launch-timeout")

  // Launcher configs
  def launcherTimeout: Option[Duration] = {
    if (launcherConfig.hasPath("timeout")) {
      Some(launcherConfig.getDuration("timeout", TimeUnit.SECONDS).seconds)
    } else {
      None
    }
  }

  // Zookeeper configs
  def zookeeperClientConfig: ZookeeperClientConfig = ZookeeperClientConfig.fromConfig(zookeeperConfig)

  // Rest API configs
  def restPort: Int = restConfig.getInt("port")
  def restPortMaxAttempts: Int = restConfig.getInt("port-max-attempts")
  def restRequestTimeout: Timeout = Timeout(
    restConfig.getDuration("request-timeout", TimeUnit.MILLISECONDS),
    TimeUnit.MILLISECONDS)

  // Kerberos configs
  def kerberosTicketCheckInterval: Long =
    kerberosConfig.getDuration("ticket-check-interval", TimeUnit.MILLISECONDS)

  // Yarn configs
  def yarnApplicationName: String = yarnConfig.getString("application-name")
  def yarnJvmArgs: mutable.Seq[String] = yarnConfig.getStringList("master.jvm.args").asScala
  def yarnMaxAttempts: Int = yarnConfig.getInt("max-attempts")
  def yarnClientThreads: Int = yarnConfig.getInt("client-threads")
  def yarnStagingDirectory(conf: Configuration, appId: String): String ={
    val basePath =
      if (yarnConfig.hasPath("staging-directory")) {
        yarnConfig.getString("staging-directory")
      } else {
        new Path(FileSystem.get(conf).getHomeDirectory, ".akkeeper").toString
      }
    new Path(basePath, appId).toString
  }

  // Yarn master configs
  def yarnMasterCpus: Int = yarnMaster.getInt("cpus")
  def yarnMasterMemory: Int = yarnMaster.getInt("memory")

  // Other
  def withMasterRole: Config = {
    config.withValue("akka.cluster.roles",
      ConfigValueFactory.fromIterable(Seq(MasterService.MasterServiceName).asJava))
  }

  def withMasterPort: Config = {
    config.withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(akkaPort))
  }

  def withPrincipalAndKeytab(principal: String, keytab: String): Config = {
    config
      .withValue("akka.kerberos.principal", ConfigValueFactory.fromAnyRef(principal))
      .withValue("akka.kerberos.keytab", ConfigValueFactory.fromAnyRef(keytab))
  }

  // Utils
  def getMapOfStrings(path: String): Map[String, String] = {
    if (config.hasPath(path)) {
      config.getConfig(path).entrySet().asScala.map(entry => {
        entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String]
      }).toMap
    } else {
      Map.empty
    }
  }

  def getListOfStrings(path: String): Seq[String] = {
    if (config.hasPath(path)) {
      config.getStringList(path).asScala
    } else {
      Seq.empty
    }
  }
}
