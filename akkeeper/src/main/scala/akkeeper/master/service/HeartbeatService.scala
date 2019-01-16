/*
 * Copyright 2017-2019 Iaroslav Zeigerman
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

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Cancellable, Props}
import akkeeper.api.{Heartbeat, TerminateMaster}
import akkeeper.config._

import scala.concurrent.duration.FiniteDuration
import HeartbeatService._

private[akkeeper] final class HeartbeatService extends Actor with ActorLogging {

  private implicit val dispatcher = context.dispatcher

  private val heartbeatConfig: HeartbeatConfig = context.system.settings.config.master.heartbeat
  private val timeoutTaskInterval: FiniteDuration = heartbeatConfig.timeout

  private var missedHeartbeatsCounter: Int = 0
  private var timeoutTaskCancellable: Option[Cancellable] = None

  override def preStart(): Unit = {
    scheduleHeartbeatTimeout()
  }

  override def postStop(): Unit = {
    timeoutTaskCancellable.foreach(_.cancel())
  }

  override def receive: Receive = {
    case Heartbeat => onHeartbeat()
    case HeartbeatTimeout => onHeartbeatTimeout()
  }

  private def onHeartbeat(): Unit = {
    log.debug("Heartbeat received")
    timeoutTaskCancellable.foreach(_.cancel())
    missedHeartbeatsCounter = 0
    scheduleHeartbeatTimeout()
  }

  private def onHeartbeatTimeout(): Unit = {
    missedHeartbeatsCounter += 1
    log.warning(s"Heartbeat timeout. $missedHeartbeatsCounter heartbeats have been missed so far")
    if (missedHeartbeatsCounter < heartbeatConfig.missedLimit) {
      scheduleHeartbeatTimeout()
    } else {
      log.error(s"The maximum number of missed heartbeats (${heartbeatConfig.missedLimit}) " +
        "has been exceeded. Terminating the master")
      context.parent ! TerminateMaster
      context.stop(self)
    }
  }

  private def scheduleHeartbeatTimeout(): Unit = {
    val task = context.system.scheduler.scheduleOnce(timeoutTaskInterval, self, HeartbeatTimeout)
    timeoutTaskCancellable = Some(task)
  }
}

object HeartbeatService {
  val HeartbeatServiceName = "heartbeatService"

  private case object HeartbeatTimeout

  private[akkeeper] def createLocal(factory: ActorRefFactory): ActorRef = {
    factory.actorOf(Props(classOf[HeartbeatService]), HeartbeatServiceName)
  }
}
