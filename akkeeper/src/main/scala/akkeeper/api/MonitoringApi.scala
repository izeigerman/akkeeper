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
package akkeeper.api

import akkeeper.common._
import spray.json.DefaultJsonProtocol

/** The base interface for all requests related to Monitoring API. */
sealed trait InstanceRequest extends WithRequestId

/** A request to retrieve the information about the instance using its ID.
  * The possible responses are:
  *
  *  - [[InstanceInfoResponse]] - contains information about the requested instance.
  *  - [[InstanceNotFound]] - if the request instance was not found.
  *  - [[OperationFailed]] - if other error occurred.
  *
  * @param instanceId the ID of the instance.
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class GetInstance(instanceId: InstanceId,
                       requestId: RequestId = RequestId()) extends InstanceRequest

/** A request to retrieve the IDs of all existing instances.
  * The possible responses are:
  *
  *  - [[InstancesList]] - contains the list of existing instances.
  *  - [[OperationFailed]] - if error occurred.
  *
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class GetInstances(requestId: RequestId = RequestId()) extends InstanceRequest

/** A request to find the instance IDs that match specific requirements.
  * The possible responses are:
  *
  *  - [[InstancesList]] - contains the list of requested instances.
  *  - [[OperationFailed]] - if error occurred.
  *
  * @param roles the list of roles the requested instance must have.
  *              An empty list means any roles. Note: an instance must
  *              include all roles enumerated in this list in order to
  *              meet the search requirements.
  * @param containerName the name of the container the requested instance
  *                      must belong to. If not specified instance with any
  *                      container will match.
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class GetInstancesBy(roles: Set[String],
                          containerName: Option[String],
                          requestId: RequestId = RequestId()) extends InstanceRequest

/** A request to terminate a running instance.
  * The possible responses are:
  *
  *  - [[InstanceTerminated]] - of the instance has been terminated successfully.
  *  - [[OperationFailed]] - if error occurred.
  *
  * @param instanceId the ID of the instance that has to be terminated.
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class TerminateInstance(instanceId: InstanceId,
                             requestId: RequestId = RequestId()) extends InstanceRequest

/** The base interface for all responses related to Monitoring API. */
sealed trait InstanceResponse extends WithRequestId

/** A response that contains an information about the requested intance.
  * This is a result of the [[GetInstance]] operation.
  *
  * @param requestId the ID of the original request.
  * @param info the information about the requested instance. See [[InstanceInfo]].
  */
case class InstanceInfoResponse(requestId: RequestId,
                                info: InstanceInfo) extends InstanceResponse

/** A response that contains the list of IDs of existing instances.
  * This is a result of the [[GetInstances]] operation.
  *
  * @param requestId the ID of the original request.
  * @param instanceIds the list of instance IDs.
  */
case class InstancesList(requestId: RequestId,
                         instanceIds: Seq[InstanceId]) extends InstanceResponse

/** A response that indicates that the request instance was not found.
  *
  * @param requestId the ID of the original request.
  * @param instanceId the requested instance ID.
  */
case class InstanceNotFound(requestId: RequestId,
                            instanceId: InstanceId) extends InstanceResponse

/** A response that indicates the successful termination of the instance.
  * This is a result of the [[TerminateInstance]] operation.
  *
  * @param requestId the ID of the original request.
  * @param instanceId the ID of the terminated intance.
  */
case class InstanceTerminated(requestId: RequestId,
                              instanceId: InstanceId) extends InstanceResponse

/** JSON (de)serialization for the Monitoring API requests and responses. */
trait MonitoringApiJsonProtocol extends DefaultJsonProtocol
  with InstanceIdJsonProtocol with RequestIdJsonProtocol
  with InstanceStatusJsonProtocol {

  implicit val getInstanceFormat = AutoRequestIdFormat(jsonFormat2(GetInstance.apply))
  implicit val getInstancesFormat = AutoRequestIdFormat(jsonFormat1(GetInstances.apply))
  implicit val getInstancesByFormat = AutoRequestIdFormat(jsonFormat3(GetInstancesBy.apply))
  implicit val terminateInstancesFormat = AutoRequestIdFormat(jsonFormat2(TerminateInstance.apply))

  implicit val instanceInfoResponseFormat = jsonFormat2(InstanceInfoResponse.apply)
  implicit val instanceListFormat = jsonFormat2(InstancesList.apply)
  implicit val instanceNotFoundFormat = jsonFormat2(InstanceNotFound.apply)
  implicit val instanceTerminatedFormat = jsonFormat2(InstanceTerminated.apply)
}

object MonitoringApiJsonProtocol extends MonitoringApiJsonProtocol
