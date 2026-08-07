package ai.queryone.extension.overwrite

final case class ManagedHdfsLoadRequest(
    workspaceOwner: String,
    targetTable: String,
    format: String,
    relativePath: String,
    options: Map[String, String])

object ManagedHdfsLoadProtocol {
  private val Command = "QUERYONE MANAGED_HDFS_LOAD"

  def render(request: ManagedHdfsLoadRequest): String = {
    ManagedHdfsCommandCodec.render(
      Command,
      ManagedHdfsCommandFields(
        request.workspaceOwner,
        request.targetTable,
        request.format,
        request.relativePath,
        request.options))
  }

  def parse(sql: String): Option[ManagedHdfsLoadRequest] = {
    ManagedHdfsCommandCodec.parse(sql, Command, "load").map { fields =>
      ManagedHdfsLoadRequest(
        fields.tenant,
        fields.table,
        fields.format,
        fields.relativePath,
        fields.options)
    }
  }

  def isCommand(sql: String): Boolean = parse(sql).nonEmpty
}
