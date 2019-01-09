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
package akkeeper.common.service

import akka.actor.{Actor, ActorLogging, ActorRef}
import akkeeper.api.WithRequestId
import akkeeper.common.RequestId
import scala.collection.mutable

private[akkeeper] case class SenderContext(sender: ActorRef, context: Option[Any])

private[akkeeper] trait RequestTrackingService extends Actor with ActorLogging {

  private val sendersById: mutable.Map[RequestId, SenderContext] = mutable.Map.empty
  private var currentServiceReceive: Option[Receive] = None

  private def getServiceReceive: Receive = {
    currentServiceReceive.getOrElse(serviceReceive)
  }

  private def trackedMessagesReceive: Receive = {
    case request: WithRequestId if trackedMessages.contains(request.getClass) =>
      sendersById.put(request.requestId, SenderContext(sender(), None))
      getServiceReceive(request)
  }

  protected def serviceReceive: Receive

  protected def trackedMessages: Set[Class[_]] = Set.empty

  protected def setOriginalSenderContext(id: RequestId, context: Any): Unit = {
    if (sendersById.contains(id)) {
      val record = sendersById(id)
      sendersById.put(id, record.copy(context = Some(context)))
    } else {
      throw ServiceException(s"Sender with ID $id was not found")
    }
  }

  protected def originalSenderContextAs[T](id: RequestId): T = {
    sendersById
      .get(id)
      .flatMap(_.context.map(_.asInstanceOf[T]))
      .getOrElse(throw ServiceException(s"No context provided for sender with ID $id"))
  }

  protected def originalSender(id: RequestId): ActorRef = {
    sendersById
      .get(id).map(_.sender)
      .getOrElse(throw ServiceException(s"Sender with ID $id was not found"))
  }

  protected def removeOriginalSender(id: RequestId): Unit = {
    sendersById.remove(id)
  }

  protected def sendAndRemoveOriginalSender[T <: WithRequestId](msg: T): Unit = {
    val recipient = originalSender(msg.requestId)
    recipient ! msg
    removeOriginalSender(msg.requestId)
  }

  protected def become(newState: Receive): Unit = {
    currentServiceReceive = Some(newState)
    context.become(trackedMessagesReceive orElse newState)
  }

  override def receive: Receive = {
    trackedMessagesReceive orElse serviceReceive
  }
}
