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
package akkeeper.deploy.yarn

import java.util.UUID

import akka.actor.Address
import akkeeper.deploy.{DeployFailed, DeploySuccessful}
import akkeeper.AwaitMixin
import akkeeper.api.InstanceId
import akkeeper.common.AkkeeperException
import akkeeper.common.config._
import akkeeper.yarn.{LocalResourceNames, YarnMasterClient}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class YarnApplicationMasterSpec extends FlatSpec with Matchers
  with MockFactory with BeforeAndAfterAll with AwaitMixin {

  private val config = ConfigFactory.load("application-container-test.conf")
  private val port = 12345
  private val selfAddress = Address("akka.tcp", "YarnApplicationMasterSpec", "localhost", port)
  private val stagingDirRoot = "/tmp/akkeepe-staging"
  private val hadoopFs = FileSystem.get(new Configuration())

  private def createAppMasterConfig(config: Config): YarnApplicationMasterConfig = {
    YarnApplicationMasterConfig(config, new YarnConfiguration(),
      "appId", selfAddress, "", principal = Some("username"))
  }

  private def createDummyResource(stagingDir: Path, resourceName: String): Unit = {
    val resource = hadoopFs
      .create(new Path(stagingDir, s"appId/$resourceName"))
    resource.write(Array[Byte](1))
    resource.close()
  }

  private def createStagingAppMasterConfig: YarnApplicationMasterConfig = {
    val stagingDirectory = new Path(stagingDirRoot, UUID.randomUUID().toString)
    hadoopFs.mkdirs(stagingDirectory)
    val configWithStaging = config
      .withValue("akkeeper.yarn.staging-directory",
        ConfigValueFactory.fromAnyRef(stagingDirectory.toString))

    createDummyResource(stagingDirectory, LocalResourceNames.AkkeeperJarName)
    createDummyResource(stagingDirectory, LocalResourceNames.UserJarName)
    createDummyResource(stagingDirectory, LocalResourceNames.KeytabName)

    createAppMasterConfig(configWithStaging)
  }

  override protected def afterAll(): Unit = {
    hadoopFs.delete(new Path(stagingDirRoot), true)
    super.afterAll()
  }

  "A YARN Application Master" should "start and stop successfully" in {
    val yarnClient = mock[YarnMasterClient]
    (yarnClient.init _).expects(*)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()
    (yarnClient.registerApplicationMaster _).expects("localhost", 0, "")
    (yarnClient.unregisterApplicationMaster _).expects(FinalApplicationStatus.SUCCEEDED, *, *)

    val masterConfig = createStagingAppMasterConfig
    val master = new YarnApplicationMaster(masterConfig, yarnClient)
    master.start()
    master.stop()
  }

  it should "start successfully and stop with error" in {
    val expectedException = new AkkeeperException("fail")

    val yarnClient = mock[YarnMasterClient]
    (yarnClient.init _).expects(*)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()
    (yarnClient.registerApplicationMaster _).expects("localhost", 0, "")
    (yarnClient.unregisterApplicationMaster _)
      .expects(FinalApplicationStatus.FAILED, expectedException.getMessage, *)

    val masterConfig = createStagingAppMasterConfig
    val master = new YarnApplicationMaster(masterConfig, yarnClient)
    master.start()
    master.stopWithError(expectedException)
  }

  private def createAllocateResponse(priorities: Seq[Int]): AllocateResponse = {
    val appAttemptId = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(0, 0), 0)
    val containers = priorities.map(p => {
      Container.newInstance(
        ContainerId.newContainerId(appAttemptId, 1),
        null, null, null, Priority.newInstance(p), null)
    })
    AllocateResponse.newInstance(
      0, Seq.empty[ContainerStatus].asJava,
      containers.asJava, Seq.empty[NodeReport].asJava,
      null, null, 3, null, null)
  }

  it should "deploy new container successfully" in {
    val container = config.akkeeper.containers(0)
    val instanceId = InstanceId(container.name)

    val yarnClient = mock[YarnMasterClient]
    (yarnClient.init _).expects(*)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()
    (yarnClient.registerApplicationMaster _).expects("localhost", 0, "")
    (yarnClient.unregisterApplicationMaster _).expects(FinalApplicationStatus.SUCCEEDED, *, *)

    (yarnClient.addContainerRequest _).expects(*)
    (yarnClient.startContainer _).expects(*, *).returns(Map.empty)
    (yarnClient.allocate _)
      .expects(*)
      .returns(createAllocateResponse(Seq(0)))
      .atLeastOnce()

    val masterConfig = createStagingAppMasterConfig
    val master = new YarnApplicationMaster(masterConfig, yarnClient)
    master.start()

    val actualResult = master.deploy(container, Seq(instanceId))
    actualResult.size shouldBe 1

    val deployResult = await(actualResult(0))
    deployResult shouldBe DeploySuccessful(instanceId)

    val stagingDirectory = masterConfig.config.getString("akkeeper.yarn.staging-directory")
    hadoopFs.exists(new Path(stagingDirectory, "appId")) shouldBe true
    master.stop()
    // Make sure that the staging directory has been cleaned up.
    hadoopFs.exists(new Path(stagingDirectory, "appId")) shouldBe false
  }

  it should "handle failed deployment properly" in {
    val container = config.akkeeper.containers(0)
    val instanceId = InstanceId(container.name)

    val yarnClient = mock[YarnMasterClient]
    (yarnClient.init _).expects(*)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()
    (yarnClient.registerApplicationMaster _).expects("localhost", 0, "")
    (yarnClient.unregisterApplicationMaster _).expects(FinalApplicationStatus.SUCCEEDED, *, *)

    (yarnClient.addContainerRequest _).expects(*)
    (yarnClient.startContainer _)
      .expects(*, *)
      .onCall(_ => throw new AkkeeperException("fail"))
    (yarnClient.allocate _)
      .expects(*)
      .returns(createAllocateResponse(Seq(0)))
      .atLeastOnce()

    val masterConfig = createStagingAppMasterConfig
    val master = new YarnApplicationMaster(masterConfig, yarnClient)
    master.start()

    val actualResult = master.deploy(container, Seq(instanceId))
    actualResult.size shouldBe 1

    val deployResult = await(actualResult(0).mapTo[DeployFailed])
    deployResult.instanceId shouldBe instanceId

    master.stop()
  }

  it should "handle the failed unregister process properly" in {
    val yarnClient = mock[YarnMasterClient]
    (yarnClient.init _).expects(*)
    (yarnClient.start _).expects()
    (yarnClient.stop _).expects()
    (yarnClient.registerApplicationMaster _).expects("localhost", 0, "")
    (yarnClient.unregisterApplicationMaster _)
      .expects(FinalApplicationStatus.SUCCEEDED, *, *)
      .onCall(_ => new AkkeeperException("fail"))

    val masterConfig = createStagingAppMasterConfig
    val master = new YarnApplicationMaster(masterConfig, yarnClient)
    master.start()
    master.stop()
  }
}

