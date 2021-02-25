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
package akkeeper.yarn

import java.io.{Closeable, InputStream}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.util.ConverterUtils

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

  private def create(fs: FileSystem,
                     status: FileStatus,
                     localResourceType: LocalResourceType,
                     localResourceVisibility: LocalResourceVisibility): LocalResource = {
    LocalResource.newInstance(
      URL.fromURI(fs.makeQualified(status.getPath).toUri),
      localResourceType, localResourceVisibility,
      status.getLen, status.getModificationTime
    )
  }

  private def copyResourceToStagingDir(srcStream: InputStream, dstPath: String): Path = {
    val dstFs = stagingDirPath.getFileSystem(conf)
    val dst = new Path(stagingDirPath, dstPath)
    withStream(dstFs.create(dst)) { out =>
      IOUtils.copy(srcStream, out)
      out.hsync()
    }
    dst
  }

  def getLocalResource(
    dstPath: Path,
    localResourceType: LocalResourceType = LocalResourceType.FILE,
    localResourceVisibility: LocalResourceVisibility = LocalResourceVisibility.APPLICATION
  ): LocalResource = {
    val dstFs = dstPath.getFileSystem(conf)
    val dstStatus = dstFs.getFileStatus(dstPath)
    create(dstFs, dstStatus, localResourceType, localResourceVisibility)
  }

  def uploadLocalResource(srcStream: InputStream, dstPath: String): LocalResource = {
    val dst = copyResourceToStagingDir(srcStream, dstPath)
    getLocalResource(dst)
  }

  def uploadLocalResource(srcPath: String, dstPath: String): LocalResource = {
    val path = new Path(srcPath)
    val srcFs = path.getFileSystem(conf)
    withStream(srcFs.open(path)) { srcStream =>
      uploadLocalResource(srcStream, dstPath)
    }
  }

  def getLocalResourceFromStagingDir(dstPath: Path): LocalResource = {
    getLocalResource(new Path(stagingDirPath, dstPath))
  }

  def getLocalResourceFromStagingDir(dstPath: String): LocalResource = {
    getLocalResourceFromStagingDir(new Path(dstPath))
  }
}
