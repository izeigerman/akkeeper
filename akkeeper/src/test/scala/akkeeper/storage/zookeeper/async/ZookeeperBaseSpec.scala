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
package akkeeper.storage.zookeeper.async

import akkeeper.storage.Storage
import org.apache.curator.test.{TestingServer => ZookeeperServer}
import org.scalatest.Suite

trait ZookeeperBaseSpec {
  this: Suite =>

  protected def withStorage[S <: Storage, T](storage: S, server: ZookeeperServer)
                                            (f: S => T): T = {
    storage.start()
    try {
      f(storage)
    } finally {
      storage.stop()
      server.stop()
    }
  }

}
