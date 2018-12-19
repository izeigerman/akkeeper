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
package akkeeper.utils.yarn

import java.io.{Closeable, InputStream}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.yarn.api.records._

private[akkeeper] final class YarnLocalResourceManager(conf: Configuration,
                                                       stagingDir: String) {

  private val stagingDirPath: Path = new Path(stagingDir)

  private def withStream[S <: Closeable, R](s: => S)(f: S => R): R = {
    val stream = s
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  private def create(fs: FileSystem, status: FileStatus): LocalResource = {
    LocalResource.newInstance(
      URL.fromURI(fs.makeQualified(status.getPath).toUri),
      LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
      status.getLen, status.getModificationTime
    )
  }

  private def copyResourceToStagingDir(dstFs: FileSystem, srcStream: InputStream,
                                       dstPath: String): Path = {
    val dst = new Path(stagingDirPath, dstPath)
    withStream(dstFs.create(dst)) { out =>
      IOUtils.copy(srcStream, out)
      out.hsync()
    }
    dst
  }

  def createLocalResource(srcStream: InputStream, dstPath: String): LocalResource = {
    val dstFs = stagingDirPath.getFileSystem(conf)
    val dst = copyResourceToStagingDir(dstFs, srcStream, dstPath)
    val dstStatus = dstFs.getFileStatus(dst)
    create(dstFs, dstStatus)
  }

  def createLocalResource(srcPath: String, dstPath: String): LocalResource = {
    val path = new Path(srcPath)
    val srcFs = path.getFileSystem(conf)
    withStream(srcFs.open(path)) { srcStream =>
      createLocalResource(srcStream, dstPath)
    }
  }

  def getExistingLocalResource(dstPath: Path): LocalResource = {
    val fs = dstPath.getFileSystem(conf)
    val dstStatus = fs.getFileStatus(new Path(stagingDirPath, dstPath))
    create(fs, dstStatus)
  }

  def getExistingLocalResource(dstPath: String): LocalResource = {
    getExistingLocalResource(new Path(dstPath))
  }
}
