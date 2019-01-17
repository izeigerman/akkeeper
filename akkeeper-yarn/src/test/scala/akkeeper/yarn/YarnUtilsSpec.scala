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
package akkeeper.yarn

import org.scalatest.{FlatSpec, Matchers}

class YarnUtilsSpec extends FlatSpec with Matchers {

  "YARN Utils" should "build a correct command" in {
    val expectedCmd = "exec {{JAVA_HOME}}/bin/java -Xmx1g " +
      "-cp akkeeper.jar:`{{HADOOP_YARN_HOME}}/bin/yarn classpath`:test1.jar:test2.jar " +
      "com.test.Main arg1 arg2 arg3 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr"
    val mainClass = "com.test.Main"
    val classPath = Seq("test1.jar", "test2.jar")
    val jvmArgs = Seq("-Xmx1g")
    val appArgs = Seq("arg1", "arg2", "arg3")
    val cmd = YarnUtils.buildCmd(mainClass, classPath, jvmArgs, appArgs).mkString(" ")
    cmd shouldBe expectedCmd
  }

  it should "handle empty arguments correctly" in {
    val expectedCmd = "exec {{JAVA_HOME}}/bin/java " +
      "-cp akkeeper.jar:`{{HADOOP_YARN_HOME}}/bin/yarn classpath` " +
      " 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr"
    val cmd = YarnUtils.buildCmd("", Seq.empty, Seq.empty, Seq.empty).mkString(" ")
    cmd shouldBe expectedCmd
  }
}
