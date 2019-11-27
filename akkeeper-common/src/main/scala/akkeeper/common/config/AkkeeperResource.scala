package akkeeper.common.config

import java.net.URI

final case class AkkeeperResource(uri: URI, localPath: String, archive: Boolean = false)

