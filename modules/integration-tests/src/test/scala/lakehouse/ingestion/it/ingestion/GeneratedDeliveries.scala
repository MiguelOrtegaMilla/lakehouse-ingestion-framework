package lakehouse.ingestion.it.ingestion

import java.nio.file.Files
import java.nio.file.attribute.FileTime

import lakehouse.ingestion.core.config.{EngineColumns, Source}
import lakehouse.ingestion.it.support.FileGenerator.Delivery
import org.apache.spark.sql.functions.col
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

/**
 * The deliveries of `FileGenerator` in the landing zone of a suite, and what a suite compares
 * with what the generator says is in them: the rows of a source in Bronze, and the records in
 * its quarantine.
 */
trait GeneratedDeliveries extends IngestionSuite {
  this: Suite with Matchers =>

  /** The files of a delivery, in the directory of a source, complete for a run that starts now. */
  protected def leave(of: Source, delivery: Delivery): Unit =
    delivery.files.foreach { file =>
      val path = directoryOf(of).resolve(file.name)
      Files.write(path, delivery.bytesOf(file))
      Files.setLastModifiedTime(path, FileTime.from(anHourAgo)): Unit
    }

  /** A row, or a record: its file, and its value under each of the columns, if it has one. */
  protected type Record = (String, Seq[Option[String]])

  /** What Bronze is to have of some deliveries, under `columns`. */
  protected def expected(columns: Seq[String], deliveries: Delivery*): Seq[Record] =
    deliveries
      .flatMap(_.files)
      .flatMap(file => file.records.map(record => file.name -> columns.map(record.get)))
      .sortBy(_.toString)

  /**
   * What Bronze has of a source, under `columns`: the rows that say they are its own, since
   * its table may be another source's as well.
   */
  protected def written(of: Source, columns: Seq[String]): Seq[Record] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .filter(col(EngineColumns.SourceId) === of.id.value)
      .select(col(EngineColumns.SourceFile) +: columns.map(col): _*)
      .collect()
      .map { row =>
        row.getString(0).split('/').last -> columns.indices.map(i => Option(row.getString(i + 1)))
      }
      .sortBy(_.toString)
      .toSeq

  protected def columnsOf(of: Source): Set[String] =
    spark.read.format("delta").load(bronze(of).uri).columns.toSet

  /** The records of a delivery that cannot be parsed: the batch, the file, the text. */
  protected def setApart(batch: Long, delivery: Delivery): Seq[(Long, String, String)] =
    delivery.files.flatMap(file => file.corrupt.map(text => (batch, file.name, text))).sorted

  protected def bytes(delivery: Delivery): Long =
    delivery.files.map(file => delivery.bytesOf(file).length.toLong).sum
}
