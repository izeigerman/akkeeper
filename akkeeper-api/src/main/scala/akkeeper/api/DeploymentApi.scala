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
package akkeeper.api

import spray.json.DefaultJsonProtocol

/** A request to deploy (launch) new instance(s) of the given container.
  * The possible responses are:
  *
  *  - [[SubmittedInstances]] - if the deployment attempt was successful.
  *  - [[ContainerNotFound]] - if the container with requested name was not found.
  *  - [[OperationFailed]] - if error occurred.
  *
  * @param name the name of the container that will be deployed.
  * @param quantity the number of instances that will be deployed.
  * @param jvmArgs the list of JVM arguments that override/extend JVM arguments
  *                from the container definition.
  * @param properties the map of properties, where the key - is a property name,
  *                   and the value is a property value. Overrides/extends properties
  *                   from the container definition.
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class DeployContainer(name: String, quantity: Int,
                           jvmArgs: Option[Seq[String]] = None,
                           properties: Option[Map[String, String]] = None,
                           requestId: RequestId = RequestId()) extends WithRequestId

/** A response that indicates a successful deployment of new instances.
  * Note: this response only indicates the successful allocation of containers and doesn't
  * imply that actors are launched successfully too.
  *
  * @param requestId the ID of the original request.
  * @param containerName the name of the container that has been deployed.
  * @param instanceIds the list of IDs of newly launched instances.
  */
case class SubmittedInstances(requestId: RequestId, containerName: String,
                              instanceIds: Seq[InstanceId]) extends WithRequestId

/** JSON (de)serialization for the Deploy API requests and responses. */
trait DeployApiJsonProtocol extends DefaultJsonProtocol
  with RequestIdJsonProtocol with InstanceIdJsonProtocol {

  implicit val deployContainerFormat = AutoRequestIdFormat(jsonFormat5(DeployContainer))
  implicit val deployedInstancesFormat = jsonFormat3(SubmittedInstances)
}

