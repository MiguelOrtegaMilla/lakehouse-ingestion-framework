package lakehouse.ingestion.engine.control

import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.Properties

import scala.util.Using

import lakehouse.ingestion.core.control.InstanceId

/** What this process is, for the audit trail: which application, and which instance of it. */
object Application {

  private val Resource = "/lakehouse-ingestion/application.properties"

  /**
   * The version of the build and the commit it was built from, as `0.1.0+3f2a9c1d7e4b`. The
   * build writes it down (`project/ApplicationVersion.scala`).
   */
  lazy val version: String = {
    val stream = Option(getClass.getResourceAsStream(Resource)).getOrElse {
      throw new IllegalStateException(
        s"$Resource is not on the classpath: the build did not write it"
      )
    }
    val properties = new Properties
    Using.resource(stream)(properties.load)
    Option(properties.getProperty("version")).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalStateException(s"$Resource has no version")
    }
  }

  /**
   * This process. The host is the kernel's name for the machine, read without asking a name
   * service: a lookup of the local host name can take seconds.
   */
  lazy val instance: InstanceId = {
    val host =
      try new String(Files.readAllBytes(Paths.get("/proc/sys/kernel/hostname")), UTF_8).trim
      catch { case _: IOException => sys.env.getOrElse("HOSTNAME", "") }
    InstanceId.of(
      host,
      ProcessHandle.current().pid(),
      Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean.getStartTime)
    )
  }
}
