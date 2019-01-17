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

/** A launch context that represents an actor that has to be launched
  * in container.
  *
  * @param name the name of the actor.
  * @param fqn the full qualified name of the actor, i.e. "com.myproject.MyActor".
  */
case class ActorLaunchContext(name: String, fqn: String)

/** Contains all the necessary information to launch a new instance in container.
  *
  * @param name the unique name of the container.
  * @param cpus the number of CPUs that will be allocated for each
  *             instance of this container.
  * @param memory the amount of RAM in MB that will be allocated for
  *               each instance of this container.
  * @param actors the list of actors that will be deployed. See [[ActorLaunchContext]].
  * @param jvmArgs the list of JVM arguments that will be passed to each instance
  *                of this container. I.e. "-Xmx1g"
  * @param jvmProperties the map of JVM properties that will be passed to each
  *                      instance of this container. This map reflects the
  *                      behaviour of the "-Dproperty=value" JVM argument.
  * @param environment the map of environment variables that will passed to each
  *                    instance of this container. The key of the map is an environment
  *                    variable name and the value is a variable's value.
  */
case class ContainerDefinition(name: String,
                               cpus: Int,
                               memory: Int,
                               actors: Seq[ActorLaunchContext],
                               jvmArgs: Seq[String] = Seq.empty,
                               jvmProperties: Map[String, String] = Map.empty,
                               environment: Map[String, String] = Map.empty)

trait ContainerDefinitionJsonProtocol extends DefaultJsonProtocol {
  implicit val actorLaunchContextFormat = jsonFormat2(ActorLaunchContext.apply)
  implicit val containerDefinitionFormat = jsonFormat7(ContainerDefinition.apply)
}

object ContainerDefinitionJsonProtocol extends ContainerDefinitionJsonProtocol
