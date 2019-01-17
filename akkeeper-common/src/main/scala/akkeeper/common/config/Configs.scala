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
package akkeeper.common.config

import java.time.{Duration => JavaDuration}
import java.util.concurrent.TimeUnit

import akkeeper.api.{ActorLaunchContext, ContainerDefinition}
import akkeeper.common.config.ConfigUtils._
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.duration._

private[akkeeper] final class AkkeeperConfig(akkeeperConfig: Config) {
  lazy val containers: Seq[ContainerDefinition] = {
    if (akkeeperConfig.hasPath("containers")) {
      val configContainers = akkeeperConfig.getConfigList("containers").asScala
      configContainers.map(ConfigUtils.containerDefinitionFromConfig)
    } else {
      Seq.empty
    }
  }
}

private[akkeeper] final class AkkeeperAkkaConfig(akkeeperAkkaConfig: Config) {
  lazy val actorSystemName: String = akkeeperAkkaConfig.getString("system-name")
  lazy val port: Int = akkeeperAkkaConfig.getInt("port")
  lazy val seedNodesNum: Int = akkeeperAkkaConfig.getInt("seed-nodes-num")
  lazy val joinClusterTimeout: FiniteDuration = akkeeperAkkaConfig.getDuration("join-cluster-timeout")
  lazy val leaveClusterTimeout: FiniteDuration = akkeeperAkkaConfig.getDuration("leave-cluster-timeout")
}

private[akkeeper] final class MasterConfig(masterConfig: Config) {
  lazy val heartbeat: HeartbeatConfig = new HeartbeatConfig(masterConfig.getConfig("heartbeat"))
}

private[akkeeper] final class HeartbeatConfig(heartbeatConfig: Config) {
  lazy val enabled: Boolean = heartbeatConfig.getBoolean("enabled")
  lazy val timeout: FiniteDuration = heartbeatConfig.getDuration("timeout")
  lazy val missedLimit: Int = heartbeatConfig.getInt("missed-limit")
}

private[akkeeper] final class MonitoringConfig(monitoringConfig: Config) {
  lazy val launchTimeout: FiniteDuration = monitoringConfig.getDuration("launch-timeout")
}

private[akkeeper] final class LauncherConfig(launcherConfig: Config) {
  lazy val timeout: Option[Duration] = {
    if (launcherConfig.hasPath("timeout")) {
      Some(launcherConfig.getDuration("timeout", TimeUnit.SECONDS).seconds)
    } else {
      None
    }
  }
}

private[akkeeper] final class YarnConfig(yarnConfig: Config) {
  lazy val applicationName: String = yarnConfig.getString("application-name")
  lazy val maxAttempts: Int = yarnConfig.getInt("max-attempts")
  lazy val clientThreads: Int = yarnConfig.getInt("client-threads")
  lazy val masterCpus: Int = yarnConfig.getInt("master.cpus")
  lazy val masterMemory: Int = yarnConfig.getInt("master.memory")
  lazy val masterJvmArgs: Seq[String] = yarnConfig.getStringList("master.jvm.args").asScala
  lazy val stagingDirectory: Option[String] =
    if (yarnConfig.hasPath("staging-directory")) {
      Some(yarnConfig.getString("staging-directory"))
    } else {
      None
    }
}

private[akkeeper] final class RestConfig(restConfig: Config) {
  lazy val port: Int = restConfig.getInt("port")
  lazy val portMaxAttempts: Int = restConfig.getInt("port-max-attempts")
  lazy val requestTimeout: FiniteDuration = restConfig.getDuration("request-timeout")
}

private[akkeeper] final class KerberosConfig(kerberosConfig: Config) {
  lazy val ticketCheckInterval: Long =
    kerberosConfig.getDuration("ticket-check-interval", TimeUnit.MILLISECONDS)
}

object ConfigUtils {
  implicit def javaDuration2ScalaDuration(value: JavaDuration): FiniteDuration = {
    Duration.fromNanos(value.toNanos)
  }

  private[config] def actorLaunchContextFromConfig(config: Config): ActorLaunchContext = {
    ActorLaunchContext(name = config.getString("name"), fqn = config.getString("fqn"))
  }

  private[config] def containerDefinitionFromConfig(config: Config): ContainerDefinition = {
    val actorsConfig = config.getConfigList("actors").asScala
    val actors = actorsConfig.map(actorLaunchContextFromConfig)
    ContainerDefinition(
      name = config.getString("name"),
      cpus = config.getInt("cpus"),
      memory = config.getInt("memory"),
      actors = actors,
      jvmArgs = config.getListOfStrings("jvm-args"),
      jvmProperties = config.getMapOfStrings("properties"),
      environment = config.getMapOfStrings("environment")
    )
  }
}
