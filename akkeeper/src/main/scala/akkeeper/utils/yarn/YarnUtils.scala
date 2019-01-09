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
package akkeeper.utils.yarn

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier
import scala.collection.JavaConverters._


private[akkeeper] object YarnUtils {

  private def buildClassPath(extraClassPath: Seq[String]): String = {
    val yarnBin = Environment.HADOOP_YARN_HOME.$$() + "/bin/yarn"
    val yarnClasspath = s"`$yarnBin classpath`"
    (LocalResourceNames.AkkeeperJarName +: yarnClasspath +: extraClassPath).mkString(":")
  }

  def buildCmd(mainClass: String,
               extraClassPath: Seq[String] = Seq.empty,
               jvmArgs: Seq[String] = Seq.empty,
               appArgs: Seq[String] = Seq.empty): List[String] = {
    val javaBin = List(Environment.JAVA_HOME.$$() + "/bin/java")
    val allJvmArgs = jvmArgs ++ List(
      "-cp", buildClassPath(extraClassPath)
    )
    List("exec") ++ javaBin ++ allJvmArgs ++ List(mainClass) ++ appArgs ++ List(
      "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
      "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
  }

  def getHdfsConfiguration: Configuration = {
    val conf = new Configuration()
    sys.env.get("HADOOP_CONF_DIR").foreach(dir => {
      conf.addResource(new Path(dir, "core-site.xml"))
      conf.addResource(new Path(dir, "hdfs-site.xml"))
    })
    conf
  }

  def getYarnConfiguration: YarnConfiguration = {
    val conf = new YarnConfiguration(getHdfsConfiguration)
    sys.env.get("YARN_CONF_DIR").foreach(dir => {
      conf.addResource(new Path(dir, "yarn-site.xml"))
    })
    conf
  }

  def loginFromKeytab(principal: String, keytab: String): UserGroupInformation = {
    UserGroupInformation.setConfiguration(getYarnConfiguration)

    val initialUser = UserGroupInformation.getLoginUser
    val yarnTokens = initialUser.getTokens.asScala
      .filter(_.getKind == AMRMTokenIdentifier.KIND_NAME)

    UserGroupInformation.loginUserFromKeytab(principal, keytab)

    val loginUser = UserGroupInformation.getLoginUser
    yarnTokens.foreach(loginUser.addToken)
    loginUser
  }

  def obtainContainerTokens(stagingDir: String,
                            hadoopConfig: Configuration): ByteBuffer = {
    val renewer = Master.getMasterPrincipal(hadoopConfig)

    val creds = new Credentials(UserGroupInformation.getCurrentUser.getCredentials)
    val hadoopFs = new Path(stagingDir)
      .getFileSystem(hadoopConfig)
    hadoopFs.addDelegationTokens(renewer, creds)

    val outstream = new DataOutputBuffer()
    creds.writeTokenStorageToStream(outstream)
    ByteBuffer.wrap(outstream.getData)
  }
}
