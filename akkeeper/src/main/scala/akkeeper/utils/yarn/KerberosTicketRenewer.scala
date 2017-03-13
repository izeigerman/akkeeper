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

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.LoggerFactory

/** Periodically checks whether the kerberos ticket has been expired, and renews
  * it if necessary.
  *
  * @param user the user whose credentials has to be checked.
  * @param checkInterval the ticket validation interval.
  */
class KerberosTicketRenewer(user: UserGroupInformation, checkInterval: Long) {

  def this(user: UserGroupInformation) = this(user, KerberosTicketRenewer.DefaultInterval)

  private val logger = LoggerFactory.getLogger(classOf[KerberosTicketRenewer])
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  def start(): Unit = {
    scheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = user.checkTGTAndReloginFromKeytab()
    }, checkInterval, checkInterval, TimeUnit.MILLISECONDS)
    logger.info("Kerberos Ticket Renewer started successfully")
  }

  def stop(): Unit = {
    scheduler.shutdown()
    logger.info("Kerberos Ticket Renewer stopped")
  }
}

object KerberosTicketRenewer {
  val DefaultInterval = 30000 // 30 seconds.
}
