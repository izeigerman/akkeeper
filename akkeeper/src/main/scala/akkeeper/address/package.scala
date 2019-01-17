package akkeeper

import akka.actor.Address
import akka.cluster.UniqueAddress
import akkeeper.api.{InstanceAddress, InstanceUniqueAddress}

package object address {
  implicit def toAkkaAddress(addr: InstanceAddress): Address = {
    Address(addr.protocol, addr.system, addr.host, addr.port)
  }

  implicit def toInstanceAddress(addr: Address): InstanceAddress = {
    InstanceAddress(addr.protocol, addr.system, addr.host, addr.port)
  }

  implicit def toAkkaUniqueAddress(addr: InstanceUniqueAddress): UniqueAddress = {
    UniqueAddress(toAkkaAddress(addr.address), addr.longUid)
  }

  implicit def toInstanceUniqueAddress(addr: UniqueAddress): InstanceUniqueAddress = {
    InstanceUniqueAddress(toInstanceAddress(addr.address), addr.longUid)
  }
}
