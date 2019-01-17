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

import com.typesafe.config.{Config, ConfigValueFactory}

import scala.collection.JavaConverters._


private[akkeeper] final class RichConfig(config: Config) {

  // Configs
  def akkeeper: AkkeeperConfig = new AkkeeperConfig(config.getConfig("akkeeper"))
  def akkeeperAkka: AkkeeperAkkaConfig = new AkkeeperAkkaConfig(config.getConfig("akkeeper.akka"))
  def master: MasterConfig = new MasterConfig(config.getConfig("akkeeper.master"))
  def monitoring: MonitoringConfig = new MonitoringConfig(config.getConfig("akkeeper.monitoring"))
  def launcher: LauncherConfig = new LauncherConfig(config.getConfig("akkeeper.launcher"))
  def yarn: YarnConfig = new YarnConfig(config.getConfig("akkeeper.yarn"))
  def zookeeper: Config = config.getConfig("akkeeper.zookeeper")
  def rest: RestConfig = new RestConfig(config.getConfig("akkeeper.api.rest"))
  def kerberos: KerberosConfig = new KerberosConfig(config.getConfig("akkeeper.kerberos"))

  // Other
  def withMasterRole: Config = {
    config.withValue("akka.cluster.roles",
      ConfigValueFactory.fromIterable(Seq("akkeeperMaster").asJava))
  }

  def withMasterPort: Config = {
    config.withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(akkeeperAkka.port))
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
