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
package akkeeper.launcher.yarn

import java.io.{File, FileOutputStream}
import java.net.URI
import java.util.UUID

import akkeeper.AwaitMixin
import akkeeper.launcher.LaunchArguments
import akkeeper.utils.ConfigUtils._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClientApplication
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try
import YarnLauncherSpec._

class YarnLauncherSpec extends FlatSpec with Matchers with MockFactory
  with AwaitMixin with BeforeAndAfterAll {

  private val stagingDirectory = "/tmp/akkeeper-staging"
  private val resourcesDirectory = "/tmp/akkeeper-test-resources"
  private val config = ConfigFactory.load()
    .withValue("akkeeper.yarn.staging-directory", ConfigValueFactory.fromAnyRef(stagingDirectory))

  override protected def afterAll(): Unit = {
    Try {
      FileUtils.deleteDirectory(new File(resourcesDirectory))
      FileSystem.get(new Configuration()).delete(new Path(stagingDirectory), true)
    }
    super.afterAll()
  }

  private def createResource(dir: String, fileName: String): String = {
    new File(dir).mkdirs()
    val pathStr = new Path(dir, fileName).toString
    val out = new FileOutputStream(pathStr)
    out.write(Array[Byte](1))
    out.flush()
    out.close()
    pathStr
  }

  private def createApplicationId(id: Int): ApplicationId = {
    ApplicationId.newInstance(System.currentTimeMillis(), id)
  }

  private def createApplicationSubmissionContext(id: Int): ApplicationSubmissionContext = {
    val context = Records.newRecord(classOf[ApplicationSubmissionContext])
    context.setApplicationId(createApplicationId(id))
    context
  }

  private def createYarnClientApplication(id: Int): YarnClientApplication = {
    val context = createApplicationSubmissionContext(id)
    new YarnClientApplication(null, context)
  }

  private def createTestLaunchArguments: LaunchArguments = {
    val resourcesDir = new Path(resourcesDirectory, UUID.randomUUID().toString).toString
    LaunchArguments(
      akkeeperJarPath = new URI(createResource(resourcesDir, "akkeeper.jar")),
      userJar = new URI(createResource(resourcesDir, "user.jar")),
      otherJars = Seq(new URI(createResource(resourcesDir, "other.jar"))),
      resources = Seq(new URI(createResource(resourcesDir, "resource"))),
      masterJvmArgs = Seq("-test-prop"),
      userConfig = Some(ConfigFactory.load("application-container-test.conf")),
      yarnQueue = Some("queue"),
      principal = Some("username"),
      keytab = Some(new URI(createResource(resourcesDir, "some.keytab")))
    )
  }

  private def validateStagingDirectory(appId: ApplicationId): Unit = {
    val directory = new Path(stagingDirectory, appId.toString)
    val hadoopFs = FileSystem.get(new Configuration())

    hadoopFs.exists(new Path(directory, "akkeeper.jar")) shouldBe true
    hadoopFs.exists(new Path(directory, "user.jar")) shouldBe true
    hadoopFs.exists(new Path(directory, "jars/other.jar")) shouldBe true
    hadoopFs.exists(new Path(directory, "resources/resource")) shouldBe true
    hadoopFs.exists(new Path(directory, "akkeeper.keytab")) shouldBe true
  }

  "A YARN Launcher" should "launch application master successfully" in {
    val yarnConfig = new YarnConfiguration()
    val yarnClient = mock[YarnLauncherClient]
    (yarnClient.init _).expects(yarnConfig)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()

    val yarnClientApp = createYarnClientApplication(1)
    val appContext = yarnClientApp.getApplicationSubmissionContext
    val appId = appContext.getApplicationId
    (yarnClient.createApplication _).expects().returns(yarnClientApp)
    (yarnClient.submitApplication _).expects(appContext).returns(appId)

    val reportGenerator = new ReportGeneratorBuilder(appId)
      .reportState(YarnApplicationState.ACCEPTED, 1)
      .reportState(YarnApplicationState.SUBMITTED, 1)
      .reportState(YarnApplicationState.RUNNING, 1)
      .build
    (yarnClient.getApplicationReport _)
      .expects(appId)
      .onCall((_: ApplicationId) => reportGenerator.getNextReport())
      .repeated(3)

    val launcher = new YarnLauncher(yarnConfig, () => yarnClient)
    val launchArgs = createTestLaunchArguments
    val result = await(launcher.launch(config, launchArgs))
    result.appId shouldBe appId.toString
    result.masterAddress.host shouldBe Some("localhost")
    result.masterAddress.port shouldBe Some(config.getInt("akkeeper.akka.port"))
    result.masterAddress.system shouldBe config.getActorSystemName

    appContext.getApplicationName shouldBe config.getYarnApplicationName

    appContext.getQueue shouldBe "queue"

    val resource = appContext.getResource
    resource.getMemorySize shouldBe config.getInt("akkeeper.yarn.master.memory")
    resource.getVirtualCores shouldBe config.getInt("akkeeper.yarn.master.cpus")

    val containerSpec = appContext.getAMContainerSpec
    val cmd = containerSpec.getCommands.asScala.mkString(" ")
    val expectedCmd =
      "exec {{JAVA_HOME}}/bin/java -Xmx2g -test-prop " +
      "-cp akkeeper.jar:`{{HADOOP_YARN_HOME}}/bin/yarn classpath` akkeeper.master.MasterMain " +
      s"--appId ${appId.toString} --config user_config.conf --principal " +
      "username 1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr"
    cmd shouldBe expectedCmd

    validateStagingDirectory(appId)
  }

  it should "throw exception if the application state is FAILED" in {
    val yarnConfig = new YarnConfiguration()
    val yarnClient = mock[YarnLauncherClient]
    (yarnClient.init _).expects(yarnConfig)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()

    val yarnClientApp = createYarnClientApplication(1)
    val appContext = yarnClientApp.getApplicationSubmissionContext
    val appId = appContext.getApplicationId
    (yarnClient.createApplication _).expects().returns(yarnClientApp)
    (yarnClient.submitApplication _).expects(appContext).returns(appId)

    val reportGenerator = new ReportGeneratorBuilder(appId)
      .reportState(YarnApplicationState.ACCEPTED, 1)
      .reportState(YarnApplicationState.SUBMITTED, 1)
      .reportState(YarnApplicationState.FAILED, 1)
      .build
    (yarnClient.getApplicationReport _)
      .expects(appId)
      .onCall((_: ApplicationId) => reportGenerator.getNextReport())
      .repeated(3)

    val launcher = new YarnLauncher(yarnConfig, () => yarnClient)
    val launchArgs = createTestLaunchArguments
    intercept[YarnLauncherException] {
      await(launcher.launch(config, launchArgs))
    }
  }
}

object YarnLauncherSpec {
  class ReportGenerator(appId: ApplicationId, states: mutable.Queue[YarnApplicationState]) {
    def getNextReport(): ApplicationReport = {
      val report = Records.newRecord(classOf[ApplicationReport])
      report.setHost("localhost")
      report.setYarnApplicationState(states.dequeue())
      report.setApplicationId(appId)
      report
    }
  }

  class ReportGeneratorBuilder(appId: ApplicationId) {
    private val states: mutable.Queue[YarnApplicationState] = mutable.Queue.empty

    def reportState(state: YarnApplicationState, times: Int): this.type = {
      states.enqueue(Array.fill(times)(state): _*)
      this
    }

    def build: ReportGenerator = {
      new ReportGenerator(appId, states)
    }
  }
}
