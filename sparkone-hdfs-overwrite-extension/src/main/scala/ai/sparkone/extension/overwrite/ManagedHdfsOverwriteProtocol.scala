package ai.sparkone.extension.overwrite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

final case class ManagedHdfsOverwriteRequest(
    tenant: String,
    sourceTable: String,
    format: String,
    relativePath: String,
    options: Map[String, String])

object ManagedHdfsOverwriteProtocol {
  private val Version = 1
  private val Command = "SPARKONE MANAGED_HDFS_OVERWRITE"
  private val CommandPattern = ("(?is)^\\s*" + Command.replace(" ", "\\s+") +
    "\\s+([A-Za-z0-9_-]+)\\s*;?\\s*$").r

  def render(request: ManagedHdfsOverwriteRequest): String = {
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    try {
      output.writeInt(Version)
      output.writeUTF(request.tenant)
      output.writeUTF(request.sourceTable)
      output.writeUTF(request.format)
      output.writeUTF(request.relativePath)
      val options = request.options.toSeq.sortBy(_._1)
      output.writeInt(options.size)
      options.foreach { case (key, value) =>
        output.writeUTF(key)
        output.writeUTF(value)
      }
    } finally {
      output.close()
    }
    val token = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes.toByteArray)
    s"$Command $token"
  }

  def parse(sql: String): Option[ManagedHdfsOverwriteRequest] = sql match {
    case CommandPattern(token) => Some(decode(token))
    case _ => None
  }

  def isCommand(sql: String): Boolean = parse(sql).nonEmpty

  private def decode(token: String): ManagedHdfsOverwriteRequest = {
    val input = try {
      new DataInputStream(new ByteArrayInputStream(Base64.getUrlDecoder.decode(token)))
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException("Invalid SparkOne managed HDFS overwrite command", e)
    }
    try {
      val version = input.readInt()
      if (version != Version) {
        throw new IllegalArgumentException(s"Unsupported SparkOne managed HDFS overwrite command version: $version")
      }
      val tenant = input.readUTF()
      val sourceTable = input.readUTF()
      val format = input.readUTF()
      val relativePath = input.readUTF()
      val optionCount = input.readInt()
      if (optionCount < 0 || optionCount > 100) {
        throw new IllegalArgumentException("Invalid SparkOne managed HDFS overwrite option count")
      }
      val options = (0 until optionCount).map(_ => input.readUTF() -> input.readUTF()).toMap
      if (input.available() != 0) {
        throw new IllegalArgumentException("Invalid trailing data in SparkOne managed HDFS overwrite command")
      }
      ManagedHdfsOverwriteRequest(tenant, sourceTable, format, relativePath, options)
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        throw new IllegalArgumentException("Invalid SparkOne managed HDFS overwrite command", e)
    } finally {
      input.close()
    }
  }
}
