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
package akkeeper.storage.zookeeper.async

import akkeeper.storage.{RecordNotFoundException, Storage}
import spray.json._

private[zookeeper] trait BaseZookeeperStorage extends Storage {

  protected def zookeeperClient: AsyncZookeeperClient

  override def start(): Unit = zookeeperClient.start()
  override def stop(): Unit = zookeeperClient.stop()

  protected def toBytes[T: JsonWriter](obj: T): Array[Byte] = {
    obj.toJson.compactPrint.getBytes("UTF-8")
  }

  protected def fromBytes[T: JsonReader](bytes: Array[Byte]): T = {
    new String(bytes, "UTF-8").parseJson.convertTo[T]
  }

  protected def notFoundToEmptySeq[T]: PartialFunction[Throwable, Seq[T]] = {
    case _: RecordNotFoundException => Seq.empty
  }
}
