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
package akkeeper.launcher

import java.net.URI

import org.scalatest.{FlatSpec, Matchers}

class LauncherMainSpec extends FlatSpec with Matchers {

  "Launcher Main" should "transform the input URI" in {
    val workingDir = System.getProperty("user.dir") + "/"
    LauncherMain.transformUri(new URI("./local/path/to.jar")) shouldBe
      new URI("file://" + workingDir + "./local/path/to.jar")
    LauncherMain.transformUri(new URI("local/path/to.jar")) shouldBe
      new URI("file://" + workingDir + "local/path/to.jar")

    LauncherMain.transformUri(new URI("/absolute/local/path/to.jar")) shouldBe
      new URI("file:///absolute/local/path/to.jar")

    LauncherMain.transformUri(new URI("file:///absolute/local/path/to.jar")) shouldBe
      new URI("file:///absolute/local/path/to.jar")

    LauncherMain.transformUri(new URI("hdfs:///absolute/local/path/to.jar")) shouldBe
      new URI("hdfs:///absolute/local/path/to.jar")
  }
}
