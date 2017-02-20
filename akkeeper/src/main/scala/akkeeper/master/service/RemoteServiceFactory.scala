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
package akkeeper.master.service

import akka.actor.{ActorSelection, ActorSystem, RootActorPath}
import akka.cluster.Cluster

trait RemoteServiceFactory {
  def actorName: String

  def createRemote(system: ActorSystem): ActorSelection = {
    val cluster = Cluster(system)
    val akkeeperMaster = cluster.state.members
      .find(m => m.hasRole(MasterService.MasterServiceName))
      .getOrElse(throw MasterServiceException("Failed to find a running Akkeeper Master instance"))
    val akkeeperMasterAddr = akkeeperMaster.address

    val basePath = RootActorPath(akkeeperMasterAddr) / "user" / MasterService.MasterServiceName
    val servicePath =
      if (actorName != MasterService.MasterServiceName) {
        basePath / actorName
      } else {
        basePath
      }
    system.actorSelection(servicePath)
  }
}
