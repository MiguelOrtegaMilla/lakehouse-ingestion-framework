import sbt._
import sbt.Keys._

/**
 * The forked JVM in which a module's tests run Spark.
 *
 * Spark 4 on Java 17 needs access to JDK internals (`sun.nio.ch`, `java.nio`, ...). `spark-submit`
 * adds the options itself. A JVM that embeds Spark, as a test JVM does, must receive them on its
 * command line.
 *
 * Without them Spark 4.1.4 still starts, and a small local workload succeeds: this was checked
 * in P1.3 by dropping every `--add-opens`, and the Delta smoke test still passed. The failures
 * come later, as `IllegalAccessError` or `InaccessibleObjectException` in the code paths that
 * reach into the JDK (direct-buffer disposal, Netty, serialisation of JDK classes). No functional
 * test can be trusted to catch a missing option, so `SparkJvmOptionsSpec` checks the list itself.
 */
object SparkTestJvm {

  /**
   * `org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptionArray()` of Spark 4.1.4,
   * verbatim and in order.
   *
   * The build cannot call that method: it runs on sbt's Scala 2.12 classpath, without Spark. So
   * the list is copied here, and `SparkJvmOptionsSpec` compares the running test JVM against
   * Spark's own list. A Spark upgrade that changes the list fails that test, not a random Spark
   * code path.
   */
  val moduleOptions: Seq[String] = Seq(
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-modules=jdk.incubator.vector",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Dio.netty.allocator.type=pooled",
    "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE",
    "--enable-native-access=ALL-UNNAMED"
  )

  /**
   * Forked tests with Spark's module options, a heap that fits the WSL budget (discovery §7.3)
   * and UTC as the JVM default zone (discovery §4: everything is UTC).
   *
   * `SPARK_LOCAL_IP` keeps Spark on loopback. Without it Spark resolves the machine's hostname
   * before reading any `SparkConf`; under WSL that lookup stalls each session start for seconds.
   */
  val settings: Seq[Setting[_]] = Seq(
    Test / fork := true,
    Test / javaOptions ++= moduleOptions ++ Seq("-Xmx1g", "-Duser.timezone=UTC"),
    Test / envVars += "SPARK_LOCAL_IP" -> "127.0.0.1"
  )
}
