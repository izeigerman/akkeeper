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
package akkeeper.yarn.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}

private[akkeeper] class YarnLauncherClient {
  private val yarnClient = YarnClient.createYarnClient()

  def init(configuration: Configuration): Unit = {
    yarnClient.init(configuration)
  }

  def start(): Unit = {
    yarnClient.start()
  }

  def stop(): Unit = {
    yarnClient.stop()
  }

  def getApplicationReport(appId: ApplicationId): ApplicationReport = {
    yarnClient.getApplicationReport(appId)
  }

  def createApplication(): YarnClientApplication = {
    yarnClient.createApplication()
  }

  def submitApplication(appContext: ApplicationSubmissionContext): ApplicationId = {
    yarnClient.submitApplication(appContext)
  }
}
