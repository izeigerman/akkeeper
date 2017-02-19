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

import java.io.{Closeable, FileInputStream, InputStream}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

private[akkeeper] final class YarnLocalResourceManager(conf: YarnConfiguration,
                                                       stagingDir: String) {

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
      ConverterUtils.getYarnUrlFromURI(fs.makeQualified(status.getPath).toUri()),
      LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
      status.getLen, status.getModificationTime
    )
  }

  private def uploadLocalResourceImpl(fs: FileSystem, srcStream: InputStream,
                                      dstPath: String): Path = {
    val dst = new Path(stagingDir, dstPath)
    withStream(fs.create(dst)) { out =>
      IOUtils.copy(srcStream, out)
      out.hsync()
    }
    dst
  }

  private def createLocalResourceImpl(fs: FileSystem, srcStream: InputStream,
                                      dstPath: String): LocalResource = {
    val dst = uploadLocalResourceImpl(fs, srcStream, dstPath)
    val dstStatus = fs.getFileStatus(dst)
    create(fs, dstStatus)
  }

  def uploadLocalResource(srcPath: String, dstPath: String): Unit = {
    val fs = FileSystem.get(conf)
    withStream(new FileInputStream(srcPath)) { srcStream =>
      uploadLocalResourceImpl(fs, srcStream, dstPath)
    }
  }

  def uploadLocalResource(srcStream: InputStream, dstPath: String): Unit = {
    val fs = FileSystem.get(conf)
    uploadLocalResourceImpl(fs, srcStream, dstPath)
  }


  def createLocalResource(srcStream: InputStream, dstPath: String): LocalResource = {
    val fs = FileSystem.get(conf)
    createLocalResourceImpl(fs, srcStream, dstPath)
  }

  def createLocalResource(srcPath: String, dstPath: String): LocalResource = {
    val fs = FileSystem.get(conf)
    withStream(new FileInputStream(srcPath)) { srcStream =>
      createLocalResourceImpl(fs, srcStream, dstPath)
    }
  }

  def createLocalResourceFromHdfs(srcPath: String, dstPath: String): LocalResource = {
    val fs = FileSystem.get(conf)
    val path = new Path(srcPath)
    withStream(fs.open(path)) { srcStream =>
      createLocalResourceImpl(fs, srcStream, dstPath)
    }
  }

  def getExistingLocalResource(dstPath: Path): LocalResource = {
    val fs = FileSystem.get(conf)
    val dstStatus = fs.getFileStatus(new Path(stagingDir, dstPath))
    create(fs, dstStatus)
  }

  def getExistingLocalResource(dstPath: String): LocalResource = {
    getExistingLocalResource(new Path(dstPath))
  }
}
