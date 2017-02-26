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
package akkeeper.utils.yarn

import java.io.File
import java.util.UUID

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class YarnLocalResourceManagerSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val hadoopConfig = new Configuration()
  private val stagingDir = s"/tmp/${UUID.randomUUID()}"
  private val hadoopFs = FileSystem.get(hadoopConfig)

  override protected def beforeAll(): Unit = {
    hadoopFs.mkdirs(new Path(stagingDir))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    hadoopFs.delete(new Path(stagingDir), true)
    super.afterAll()
  }

  private def validateLocalResource(resource: LocalResource, expectedPath: String): Unit = {
    resource.getType shouldBe LocalResourceType.FILE
    resource.getVisibility shouldBe LocalResourceVisibility.APPLICATION
    resource.getResource.getFile shouldBe expectedPath
    resource.getResource.getScheme shouldBe "file"

    val file = new File(expectedPath)
    file.exists() shouldBe true
  }

  private def validateResourcePayload(expectedResource: String,
                                      actualResourcePath: String): Unit = {
    val expectedStream = getClass.getResourceAsStream(expectedResource)
    val actualStream = hadoopFs.open(new Path(actualResourcePath))
    IOUtils.contentEquals(expectedStream, actualStream) shouldBe true
  }

  "YARN Local Resource Manager" should "create a local resource properly (using path)" in {
    val manager = new YarnLocalResourceManager(hadoopConfig, stagingDir)
    val resource = getClass.getResource("/application-container-test.conf").getPath
    val expectedFileName = UUID.randomUUID().toString
    val expectedPath = new Path(stagingDir, expectedFileName).toString

    val actualResult = manager.createLocalResource(resource, expectedFileName)
    validateLocalResource(actualResult, expectedPath)
    validateResourcePayload("/application-container-test.conf", expectedPath)
  }

  it should "create a local resource properly (using stream)" in {
    val manager = new YarnLocalResourceManager(hadoopConfig, stagingDir)
    val resource = getClass.getResourceAsStream("/application-container-test.conf")
    val expectedFileName = UUID.randomUUID().toString
    val expectedPath = new Path(stagingDir, expectedFileName).toString

    val actualResult = manager.createLocalResource(resource, expectedFileName)
    validateLocalResource(actualResult, expectedPath)
    validateResourcePayload("/application-container-test.conf", expectedPath)
    resource.close()
  }

  it should "create a local resource properly (from HDFS)" in {
    val manager = new YarnLocalResourceManager(hadoopConfig, stagingDir)
    val resource = getClass.getResource("/application-container-test.conf").getPath
    val expectedFileName = UUID.randomUUID().toString
    val expectedPath = new Path(stagingDir, expectedFileName).toString

    manager.uploadLocalResource(resource, expectedFileName)
    val newExpectedFileName = UUID.randomUUID().toString
    val newExpectedPath = new Path(stagingDir, newExpectedFileName).toString
    val actualResult = manager.createLocalResourceFromHdfs(expectedPath, newExpectedFileName)
    validateLocalResource(actualResult, newExpectedPath)
    validateResourcePayload("/application-container-test.conf", newExpectedPath)
  }

  it should "only upload a local resource (using path)" in {
    val manager = new YarnLocalResourceManager(hadoopConfig, stagingDir)
    val resource = getClass.getResource("/application-container-test.conf").getPath
    val expectedFileName = UUID.randomUUID().toString
    val expectedPath = new Path(stagingDir, expectedFileName).toString

    manager.uploadLocalResource(resource, expectedFileName)
    validateResourcePayload("/application-container-test.conf", expectedPath)
  }

  it should "only upload a local resource (using stream)" in {
    val manager = new YarnLocalResourceManager(hadoopConfig, stagingDir)
    val resource = getClass.getResourceAsStream("/application-container-test.conf")
    val expectedFileName = UUID.randomUUID().toString
    val expectedPath = new Path(stagingDir, expectedFileName).toString

    manager.uploadLocalResource(resource, expectedFileName)
    validateResourcePayload("/application-container-test.conf", expectedPath)
    resource.close()
  }

  it should "return the existing local resource" in {
    val manager = new YarnLocalResourceManager(hadoopConfig, stagingDir)
    val resource = getClass.getResource("/application-container-test.conf").getPath
    val expectedFileName = UUID.randomUUID().toString
    val expectedPath = new Path(stagingDir, expectedFileName).toString

    manager.uploadLocalResource(resource, expectedFileName)
    val actualResult = manager.getExistingLocalResource(expectedFileName)
    validateLocalResource(actualResult, expectedPath)
    validateResourcePayload("/application-container-test.conf", expectedPath)
  }
}
