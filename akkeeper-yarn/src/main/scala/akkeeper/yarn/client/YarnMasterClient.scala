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
package akkeeper.yarn.client

import java.nio.ByteBuffer

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.JavaConverters._

private[akkeeper] class YarnMasterClient {
  private val amrmClient = AMRMClient.createAMRMClient[ContainerRequest]()
  private val nmClient = NMClient.createNMClient()

  def init(config: YarnConfiguration): Unit = {
    amrmClient.init(config)
    nmClient.init(config)
  }

  def start(): Unit = {
    amrmClient.start()
    nmClient.start()
  }

  def stop(): Unit = {
    nmClient.stop()
    amrmClient.stop()
  }

  def addContainerRequest(request: ContainerRequest): Unit = {
    amrmClient.addContainerRequest(request)
  }

  def allocate(progressIndicator: Float): AllocateResponse = {
    amrmClient.allocate(progressIndicator)
  }

  def releaseAssignedContainer(containerId: ContainerId): Unit = {
    amrmClient.releaseAssignedContainer(containerId)
  }

  def startContainer(container: Container,
                     containerLaunchContext: ContainerLaunchContext): Map[String, ByteBuffer] = {
    nmClient.startContainer(container, containerLaunchContext).asScala.toMap
  }

  def registerApplicationMaster(appHostName: String,
                                appHostPort: Int,
                                appTrackingUrl: String): Unit = {
    amrmClient.registerApplicationMaster(appHostName, appHostPort, appTrackingUrl)
  }

  def unregisterApplicationMaster(appStatus: FinalApplicationStatus,
                                  appMessage: String,
                                  appTrackingUrl: String): Unit = {
    amrmClient.unregisterApplicationMaster(appStatus, appMessage, appTrackingUrl)
  }
}
