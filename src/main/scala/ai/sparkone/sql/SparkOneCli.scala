package ai.sparkone.sql

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.io.Source

object SparkOneCli {
  def main(args: Array[String]): Unit = {
    val script = args.toList match {
      case Nil | "-" :: Nil =>
        Source.fromInputStream(System.in, "UTF-8").mkString
      case path :: Nil =>
        new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
      case _ =>
        Console.err.println("Usage: spark-one [script-file|-]")
        sys.exit(2)
    }

    new SparkOneCompiler().compile(script).foreach { statement =>
      println(statement.sql + ";")
    }
  }
}
