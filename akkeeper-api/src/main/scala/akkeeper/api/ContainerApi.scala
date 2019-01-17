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

import spray.json._

/** The base interface for all requests related to Container API. */
sealed trait ContainerRequest extends WithRequestId

/** A container creation request.
  * The possible responses are:
  *
  *  - [[ContainerCreated]] - if the given container was created successfully.
  *  - [[ContainerAlreadyExists]] - if a container with the same name already exists.
  *  - [[OperationFailed]] - if other error occurred.
  *
  * @param container the container definition. See [[ContainerDefinition]].
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class CreateContainer(container: ContainerDefinition,
                           requestId: RequestId = RequestId()) extends ContainerRequest

/** A container update request.
  * The possible responses are:
  *
  *  - [[ContainerUpdated]] - if the given container was updated successfully.
  *  - [[ContainerNotFound]] - if the container with the given name was not found.
  *  - [[OperationFailed]] - if other error occurred.
  *
  * @param container the updated definition of the existing container. See [[ContainerDefinition]].
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class UpdateContainer(container: ContainerDefinition,
                           requestId: RequestId = RequestId()) extends ContainerRequest

/** A request to retrieve the definition of the container with the given name.
  * The possible responses are:
  *
  *  - [[ContainerGetResult]] - contains definition of the requested container.
  *  - [[ContainerNotFound]] - if the container with the given name was not found.
  *  - [[OperationFailed]] - if other error occurred.
  *
  * @param name the container's name
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class GetContainer(name: String,
                        requestId: RequestId = RequestId()) extends ContainerRequest

/** A request to retrieve names of all existing containers.
  * The possible responses are:
  *
  *  - [[ContainersList]] - contains list of existing container names.
  *  - [[OperationFailed]] - if error occurred.
  *
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class GetContainers(requestId: RequestId = RequestId()) extends ContainerRequest

/** A container delete request.
  * The possible responses are:
  *
  *  - [[ContainerDeleted]] - if the given container was deleted successfully.
  *  - [[ContainerNotFound]] - if the container with the given name was not found.
  *  - [[OperationFailed]] - if other error occurred.
  *
  * @param name the name of the container that has to be deleted.
  * @param requestId the optional request ID. If not specified a random
  *                  ID will be generated.
  */
case class DeleteContainer(name: String,
                           requestId: RequestId = RequestId()) extends ContainerRequest

/** The base interface for all responses related to Container API. */
sealed trait ContainerResponse extends WithRequestId

/** A response that contains the list of existing container names as a result
  * of the [[GetContainers]] request.
  *
  * @param requestId the ID of the original request.
  * @param containers the list of container names.
  */
case class ContainersList(requestId: RequestId, containers: Seq[String]) extends ContainerResponse

/** A result of the [[GetContainer]] request. Contains the definition of the request
  * container.
  *
  * @param requestId the ID of the original request.
  * @param container the definition of the requested container.
  */
case class ContainerGetResult(requestId: RequestId,
                              container: ContainerDefinition) extends ContainerResponse

/** A response that indicates that the requested container was not found.
  *
  * @param requestId the ID of the original request.
  * @param name the name of the requested container.
  */
case class ContainerNotFound(requestId: RequestId, name: String) extends ContainerResponse

/** A response that indicates that the container with the given name already exists.
  * This is one of the possible responses to the [[CreateContainer]] request.
  *
  * @param requestId the ID of the original request.
  * @param name the name of the container.
  */
case class ContainerAlreadyExists(requestId: RequestId, name: String)  extends ContainerResponse

/** A response that indicates a successful creation of the container.
  * This is a result of the [[CreateContainer]] operation.
  *
  * @param requestId the ID of the original request.
  * @param name the name of the created container.
  */
case class ContainerCreated(requestId: RequestId, name: String) extends ContainerResponse

/** A response that indicates a successful update of the container.
  * This is a result of the [[UpdateContainer]] operation.
  *
  * @param requestId the ID of the original request.
  * @param name the name of the updated container.
  */
case class ContainerUpdated(requestId: RequestId, name: String) extends ContainerResponse

/** A response that indicates a successful removing of the container.
  * This is a result of the [[DeleteContainer]] operation.
  *
  * @param requestId the ID of the original request.
  * @param name the name of the removed container.
  */
case class ContainerDeleted(requestId: RequestId, name: String) extends ContainerResponse

/** JSON (de)serialization for the Container API requests and responses. */
trait ContainerApiJsonProtocol extends DefaultJsonProtocol
  with RequestIdJsonProtocol with ContainerDefinitionJsonProtocol {

  implicit val createContainerFormat = AutoRequestIdFormat(jsonFormat2(CreateContainer))
  implicit val updateContainerFormat = AutoRequestIdFormat(jsonFormat2(UpdateContainer))
  implicit val getContainerFormat = AutoRequestIdFormat(jsonFormat2(GetContainer))
  implicit val getContainersFormat = AutoRequestIdFormat(jsonFormat1(GetContainers))
  implicit val deleteContainersFormat = AutoRequestIdFormat(jsonFormat2(DeleteContainer))

  implicit val containersListFormat = jsonFormat2(ContainersList)
  implicit val containersGetResultFormat = jsonFormat2(ContainerGetResult)
  implicit val containerNotFoundFormat = jsonFormat2(ContainerNotFound)
  implicit val containerAlreadyExistsFormat = jsonFormat2(ContainerAlreadyExists)
  implicit val containerCreatedFormat = jsonFormat2(ContainerCreated)
  implicit val containerUpdatedFormat = jsonFormat2(ContainerUpdated)
  implicit val containerDeletedFormat = jsonFormat2(ContainerDeleted)
}

