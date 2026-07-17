package ai.sparkone.extension.overwrite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.Base64

final case class ManagedHdfsOverwriteRequest(
    tenant: String,
    sourceTable: String,
    format: String,
    relativePath: String,
    options: Map[String, String])

object ManagedHdfsOverwriteProtocol {
  private val Command = "SPARKONE MANAGED_HDFS_OVERWRITE"

  def render(request: ManagedHdfsOverwriteRequest): String = {
    ManagedHdfsCommandCodec.render(
      Command,
      ManagedHdfsCommandFields(
        request.tenant,
        request.sourceTable,
        request.format,
        request.relativePath,
        request.options))
  }

  def parse(sql: String): Option[ManagedHdfsOverwriteRequest] = {
    ManagedHdfsCommandCodec.parse(sql, Command, "overwrite").map { fields =>
      ManagedHdfsOverwriteRequest(
        fields.tenant,
        fields.table,
        fields.format,
        fields.relativePath,
        fields.options)
    }
  }

  def isCommand(sql: String): Boolean = parse(sql).nonEmpty
}

private[overwrite] final case class ManagedHdfsCommandFields(
    tenant: String,
    table: String,
    format: String,
    relativePath: String,
    options: Map[String, String])

private[overwrite] object ManagedHdfsCommandCodec {
  private val Version = 1

  def render(command: String, fields: ManagedHdfsCommandFields): String = {
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    try {
      output.writeInt(Version)
      output.writeUTF(fields.tenant)
      output.writeUTF(fields.table)
      output.writeUTF(fields.format)
      output.writeUTF(fields.relativePath)
      val options = fields.options.toSeq.sortBy(_._1)
      output.writeInt(options.size)
      options.foreach { case (key, value) =>
        output.writeUTF(key)
        output.writeUTF(value)
      }
    } finally {
      output.close()
    }
    val token = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes.toByteArray)
    s"$command $token"
  }

  def parse(
      sql: String,
      command: String,
      operation: String): Option[ManagedHdfsCommandFields] = {
    val commandPattern = ("(?is)^\\s*" + command.replace(" ", "\\s+") +
      "\\s+([A-Za-z0-9_-]+)\\s*;?\\s*$").r
    sql match {
      case commandPattern(token) => Some(decode(token, operation))
      case _ => None
    }
  }

  private def decode(token: String, operation: String): ManagedHdfsCommandFields = {
    val input = try {
      new DataInputStream(new ByteArrayInputStream(Base64.getUrlDecoder.decode(token)))
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Invalid SparkOne managed HDFS $operation command", e)
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
        throw new IllegalArgumentException(s"Invalid SparkOne managed HDFS $operation option count")
      }
      val options = (0 until optionCount).map(_ => input.readUTF() -> input.readUTF()).toMap
      if (input.available() != 0) {
        throw new IllegalArgumentException(s"Invalid trailing data in SparkOne managed HDFS $operation command")
      }
      ManagedHdfsCommandFields(tenant, sourceTable, format, relativePath, options)
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        throw new IllegalArgumentException(s"Invalid SparkOne managed HDFS $operation command", e)
    } finally {
      input.close()
    }
  }
}
