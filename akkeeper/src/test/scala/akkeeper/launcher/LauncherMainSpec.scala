package akkeeper.launcher

import java.net.URI

import org.scalatest.{FlatSpec, Matchers}

class LauncherMainSpec extends FlatSpec with Matchers {

  "Launcher Main" should "transform the input URI" in {
    val workingDir = System.getProperty("user.dir") + "/"
    LauncherMain.transformUri(new URI("./local/path/to.jar")) shouldBe
      new URI("file://" + workingDir + "./local/path/to.jar")
    LauncherMain.transformUri(new URI("local/path/to.jar")) shouldBe
      new URI("file://" + workingDir + "local/path/to.jar")

    LauncherMain.transformUri(new URI("/absolute/local/path/to.jar")) shouldBe
      new URI("file:///absolute/local/path/to.jar")

    LauncherMain.transformUri(new URI("file:///absolute/local/path/to.jar")) shouldBe
      new URI("file:///absolute/local/path/to.jar")

    LauncherMain.transformUri(new URI("hdfs:///absolute/local/path/to.jar")) shouldBe
      new URI("hdfs:///absolute/local/path/to.jar")
  }
}
