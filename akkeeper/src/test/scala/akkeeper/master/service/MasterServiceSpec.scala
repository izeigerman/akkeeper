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
package akkeeper.master.service

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import akkeeper._
import akkeeper.api._
import akkeeper.common.InstanceId
import akkeeper.deploy.{DeployClient, DeploySuccessful}
import akkeeper.storage.InstanceStorage
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalamock.matchers.ArgThat
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import MasterServiceSpec._
import MonitoringServiceSpec._

class MasterServiceSpec extends FlatSpecLike with Matchers with MockFactory {

  "A Master Service" should "initialize successfully and create a new cluster" in {
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()

    new MasterServiceTestRunner() {
      override def test(): Unit = {
        val service = MasterService.createLocal(system, deployClient, storage)

        val getInstances = GetInstances()
        service ! getInstances
        val response = expectMsgClass(classOf[InstancesList])
        response.requestId shouldBe getInstances.requestId
        response.instanceIds shouldBe empty

        gracefulActorStop(service)
      }
    }.run()
  }

  ignore should "deploy initial instances" in {
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.deploy _)
      .expects(*, *)
      .returns(Seq(Future successful DeploySuccessful(InstanceId("container1"))))

    val instancesConfig = ConfigFactory
      .parseString(
        """
          |akkeeper.instances = [
          |  {
          |    name = "container1"
          |    quantity = 1
          |  }
          |]
        """.stripMargin)
      .withFallback(ConfigFactory.load("application-container-test.conf"))

    new MasterServiceTestRunner(instancesConfig) {
      private val getInstancesInterval: Long = 3000 // milliseconds

      @tailrec
      private def validateGetInstances(service: ActorRef,
                                       numRetries: Int = 3): Unit = {
        val getInstances = GetInstances()
        service ! getInstances
        val response = expectMsgClass(classOf[InstancesList])
        if (response.instanceIds.isEmpty) {
          val newRetries = numRetries - 1
          newRetries should be > 0
          Thread.sleep(getInstancesInterval)
          validateGetInstances(service, newRetries)
        }
      }

      override def test(): Unit = {
        val service = MasterService.createLocal(system, deployClient, storage)
        validateGetInstances(service)
        gracefulActorStop(service)
      }
    }.run()
  }

  it should "initialize successfully and join the existing cluster" in {
    new MasterServiceTestRunner() {
      override def test(): Unit = {
        val selfAddr = Cluster(system).selfUniqueAddress
        val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
        val storage = mock[InstanceStorage]
        (storage.start _).expects()
        (storage.stop _).expects()
        (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
        (storage.getInstance _).expects(instance.instanceId).returns(Future successful instance)

        val deployClient = mock[DeployClient]
        (deployClient.start _).expects()
        (deployClient.stop _).expects()

        val service = MasterService.createLocal(system, deployClient, storage)

        val getInstances = GetInstances()
        service ! getInstances
        val response = expectMsgClass(classOf[InstancesList])
        response.requestId shouldBe getInstances.requestId
        response.instanceIds.size shouldBe 1

        gracefulActorStop(service)
      }
    }.run()
  }

  it should "proxy deploy and container requests" in {
    val storage = mock[InstanceStorage]
    (storage.start _).expects()
    (storage.stop _).expects()
    (storage.getInstances _).expects().returns(Future successful Seq.empty)

    val instanceIds = (0 until 2).map(_ => InstanceId("container1"))
    val deployFutures = instanceIds.map(id => Future successful DeploySuccessful(id))
    val deployClient = mock[DeployClient]
    (deployClient.start _).expects()
    (deployClient.stop _).expects()
    (deployClient.deploy _)
      .expects(*, new ArgThat[Seq[InstanceId]](ids => ids.size == 2))
      .returns(deployFutures)

    new MasterServiceTestRunner() {
      override def test(): Unit = {
        val service = MasterService.createLocal(system, deployClient, storage)

        val getContainers = GetContainers()
        service ! getContainers
        val container = expectMsgClass(classOf[ContainersList])
        container.requestId shouldBe getContainers.requestId
        container.containers.size shouldBe 2
        container.containers should contain allOf("container1", "container2")

        val deployContainer = DeployContainer("container1", 2)
        service ! deployContainer
        val deployResult = expectMsgClass(classOf[SubmittedInstances])
        deployResult.requestId shouldBe deployContainer.requestId
        deployResult.containerName shouldBe "container1"
        deployResult.instanceIds.size shouldBe 2

        gracefulActorStop(service)
      }
    }.run()
  }

  it should "shutdown the Actor system if the init process fails" in {
    new MasterServiceTestRunner() {
      override def test(): Unit = {
        val selfAddr = Cluster(system).selfUniqueAddress
        val instance = createInstanceInfo("container").copy(address = Some(selfAddr))
        val storage = mock[InstanceStorage]
        (storage.start _).expects()
        (storage.stop _).expects()
        (storage.getInstances _).expects().returns(Future successful Seq(instance.instanceId))
        (storage.getInstance _)
          .expects(instance.instanceId)
          .returns(Future failed new AkkeeperException(""))

        val deployClient = mock[DeployClient]
        (deployClient.start _).expects()
        (deployClient.stop _).expects()
        (deployClient.stopWithError _).expects(*)

        MasterService.createLocal(system, deployClient, storage)

        Await.result(system.whenTerminated, 3 seconds)
      }
    }.run()
  }

  it should "shutdown the Actor system if the termination request has been received" in {
    new MasterServiceTestRunner() {
      override def test(): Unit = {
        val storage = mock[InstanceStorage]
        (storage.start _).expects()
        (storage.stop _).expects()
        (storage.getInstances _).expects().returns(Future successful Seq.empty)

        val deployClient = mock[DeployClient]
        (deployClient.start _).expects()
        (deployClient.stop _).expects()

        val service = MasterService.createLocal(system, deployClient, storage)

        val getInstances = GetInstances()
        service ! getInstances
        val response = expectMsgClass(classOf[InstancesList])
        response.requestId shouldBe getInstances.requestId
        response.instanceIds shouldBe empty

        service ! TerminateMaster

        Await.result(system.whenTerminated, 3 seconds)
      }
    }.run()
  }
}

object MasterServiceSpec {

  def testConfig(config: Config): Config = {
    config
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(new Integer(0)))
      .withValue("akka.test.timefactor", ConfigValueFactory.fromAnyRef(new Integer(3)))
      .withValue("akka.cluster.seed-node-timeout", ConfigValueFactory.fromAnyRef("2s"))
  }

  /** A custom test runner that produces a new Actor System for each test
    * in order isolate tests from each other.
    */
  abstract class MasterServiceTestRunner(system: ActorSystem)
    extends TestKit(system) with ImplicitSender with ActorTestUtils {

    def this(config: Config) = this(ActorSystem("MasterServiceSpec-" + System.currentTimeMillis(),
      testConfig(config)))

    def this() = this(ActorSystem("MasterServiceSpec-" + System.currentTimeMillis(),
      testConfig(ConfigFactory.load("application-container-test.conf"))))

    def test(): Unit

    def run(): Unit = {
      test()
      system.terminate()
    }
  }
}
