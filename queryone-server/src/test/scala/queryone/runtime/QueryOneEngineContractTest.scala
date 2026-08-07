package queryone.runtime

import queryone.identity.TenantContext
import queryone.sql.CompileException
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.{Files, Path}
import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement, Types}
import scala.collection.JavaConverters._

/**
 * Engine-neutral behavior checks. Concrete fixtures run this same contract against Local and Kyuubi.
 */
abstract class QueryOneEngineContract {
  private val tenant = TenantContext.development("contract-user")

  protected def withEngine(body: QueryOneEngine => Unit): Unit

  @Test
  def compilesReadOnlySqlWithTheSameSparkSql(): Unit = withEngine { engine =>
    val statements = engine.compile(tenant, "select 1 as id;")

    assertEquals(1, statements.size)
    assertEquals("select 1 as id", statements.head.sql)
  }

  @Test
  def rejectsNativeCreateTableBeforeExecution(): Unit = withEngine { engine =>
    try {
      engine.compile(tenant, "create table default.contract_target (id int) using parquet")
      fail("Expected native CREATE TABLE to be rejected")
    } catch {
      case error: CompileException =>
        assertTrue(error.getMessage.contains("native read-only SQL"))
    }
  }

  @Test
  def runsAReadOnlyQueryAndReturnsOneRow(): Unit = withEngine { engine =>
    val result = engine.run(tenant, "select 1 as id", 10, SessionMode.TenantShared)

    assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
    assertEquals(1, result.statements.size)
    assertEquals(Seq(Seq("1")), result.statements.head.rows)
    assertEquals(Seq("id"), result.statements.head.schema.map(_.name))
  }

  @Test
  def stopsAfterTheFirstFailedStatement(): Unit = withEngine { engine =>
    val result = engine.run(
      tenant,
      "create table default.contract_target (id int) using parquet; select 2 as id;",
      10,
      SessionMode.TenantShared)

    assertFalse(result.success)
    assertEquals(1, result.statements.size)
    assertTrue(result.stoppedEarly)
  }
}

final class LocalSparkEngineContractTest extends QueryOneEngineContract {
  override protected def withEngine(body: QueryOneEngine => Unit): Unit = {
    val root = Files.createTempDirectory("queryone-local-engine-contract-")
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder()
      .appName("QueryOne Local Engine Contract Test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .getOrCreate()
    val engine = new LocalSparkEngine(
      "local",
      "Local",
      new QueryOneRuntime(spark),
      Map.empty)
    try {
      body(engine)
    } finally {
      engine.close()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      deleteRecursively(root)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}

final class KyuubiSparkEngineContractTest extends QueryOneEngineContract {
  override protected def withEngine(body: QueryOneEngine => Unit): Unit = {
    val connection = new ContractJdbcConnection
    val engine = new KyuubiJdbcEngine(
      "kyuubi",
      "Kyuubi",
      KyuubiJdbcConfig(
        url = "jdbc:kyuubi://contract-test:10009/default",
        user = None,
        password = None,
        driver = "unused-for-test",
        properties = Map.empty),
      connectionFactory = _ => connection.connection)
    try {
      body(engine)
    } finally {
      engine.close()
    }
  }
}

private final class ContractJdbcConnection {
  @volatile private var closed = false

  private def resultSet: ResultSet = {
    var consumed = false
    proxyWithArgs(classOf[ResultSet]) { (method, args) =>
      method.getName match {
        case "getMetaData" => metadata
        case "next" =>
          val available = !consumed
          consumed = true
          Boolean.box(available)
        case "getString" => "1"
        case "getObject" => Int.box(1)
        case "wasNull" => Boolean.box(false)
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
  }

  private def metadata: ResultSetMetaData = proxyWithArgs(classOf[ResultSetMetaData]) { (method, args) =>
    method.getName match {
      case "getColumnCount" => Int.box(1)
      case "getColumnLabel" | "getColumnName" => "id"
      case "getColumnTypeName" => "int"
      case "getColumnType" => Int.box(Types.INTEGER)
      case "isNullable" => Int.box(ResultSetMetaData.columnNullable)
      case _ => defaultValue(method.getReturnType)
    }
  }

  private def statement: Statement = {
    var currentResultSet: ResultSet = null
    proxyWithArgs(classOf[Statement]) { (method, args) =>
      method.getName match {
        case "execute" | "executeQuery" =>
          currentResultSet = resultSet
          if (method.getName == "execute") Boolean.box(true) else currentResultSet
        case "getResultSet" => currentResultSet
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
  }

  val connection: Connection = proxyWithArgs(classOf[Connection]) { (method, args) =>
    method.getName match {
      case "createStatement" => statement
      case "isClosed" => Boolean.box(closed)
      case "isValid" => Boolean.box(!closed)
      case "close" =>
        closed = true
        null
      case _ => defaultValue(method.getReturnType)
    }
  }

  private def proxyWithArgs[T](interfaceClass: Class[T])(body: (Method, Array[AnyRef]) => AnyRef): T = {
    Proxy.newProxyInstance(
      interfaceClass.getClassLoader,
      Array(interfaceClass),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
          body(method, Option(args).getOrElse(Array.empty[AnyRef]))
        }
      }).asInstanceOf[T]
  }

  private def defaultValue(returnType: Class[_]): AnyRef = {
    if (!returnType.isPrimitive || returnType == java.lang.Void.TYPE) null
    else if (returnType == java.lang.Boolean.TYPE) Boolean.box(false)
    else if (returnType == java.lang.Byte.TYPE) Byte.box(0.toByte)
    else if (returnType == java.lang.Short.TYPE) Short.box(0.toShort)
    else if (returnType == java.lang.Integer.TYPE) Int.box(0)
    else if (returnType == java.lang.Long.TYPE) Long.box(0L)
    else if (returnType == java.lang.Float.TYPE) Float.box(0f)
    else if (returnType == java.lang.Double.TYPE) Double.box(0d)
    else if (returnType == java.lang.Character.TYPE) Char.box(0.toChar)
    else null
  }
}
