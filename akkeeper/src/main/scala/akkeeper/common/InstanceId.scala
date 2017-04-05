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
package akkeeper.common

import java.util.UUID

import spray.json._

/** Represents a unique ID of the instance.
  *
  * @param containerName the name of the container this instance
  *                      belongs to.
  * @param uuid the unique ID of the instance.
  */
case class InstanceId(containerName: String, uuid: UUID) {
  override def toString: String = containerName + "-" + uuid.toString
}

object InstanceId {
  def apply(containerName: String): InstanceId = InstanceId(containerName, UUID.randomUUID())
  def fromString(str: String): InstanceId = {
    val split = str.split("-", 2)
    val (containerName, uuid) = (split(0), split(1))
    InstanceId(containerName, UUID.fromString(uuid))
  }
}

trait InstanceIdJsonProtocol extends DefaultJsonProtocol {
  implicit val instanceIdFormat = new JsonFormat[InstanceId] {
    override def write(obj: InstanceId): JsValue = JsString(obj.toString)
    override def read(json: JsValue): InstanceId = InstanceId.fromString(json.convertTo[String])
  }
}

object InstanceIdJsonProtocol extends InstanceIdJsonProtocol
