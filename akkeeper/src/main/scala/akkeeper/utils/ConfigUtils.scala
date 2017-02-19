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
package akkeeper.utils

import akkeeper.api.DeployContainer
import akkeeper.common.ContainerDefinition
import akkeeper.storage.zookeeper.ZookeeperClientConfig
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConverters._

private[akkeeper] object ConfigUtils {
  implicit class ConfigDecorator(config: Config) {
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

    def getActorSystemName: String = {
      config.getString("akkeeper.actor-system-name")
    }

    def getYarnConfig: Config = {
      config.getConfig("akkeeper.yarn")
    }

    def getYarnApplicationName: String = {
      getYarnConfig.getString("application-name")
    }

    def getYarnStagingDirectory(conf: Configuration, appId: String): String ={
      val basePath =
        if (getYarnConfig.hasPath("staging-directory")) {
          getYarnConfig.getString("staging-directory")
        } else {
          new Path(FileSystem.get(conf).getHomeDirectory, ".akkeeper").toString
        }
      new Path(basePath, appId).toString
    }

    def getZookeeperClientConfig: ZookeeperClientConfig = {
      ZookeeperClientConfig.fromConfig(config.getConfig("akkeeper.zookeeper"))
    }

    def getContainers: Seq[ContainerDefinition] = {
      if (config.hasPath("akkeeper.containers")) {
        val configContainers = config.getConfigList("akkeeper.containers").asScala
        configContainers.map(ContainerDefinition.fromConfig(_))
      } else {
        Seq.empty
      }
    }

    def getDeployRequests: Seq[DeployContainer] = {
      if (config.hasPath("akkeeper.instances")) {
        val instances = config.getConfigList("akkeeper.instances").asScala
        instances.map(conf => {
          val containerName = conf.getString("name")
          val quantity = conf.getInt("quantity")
          DeployContainer(containerName, quantity)
        })
      } else {
        Seq.empty
      }
    }
  }
}
