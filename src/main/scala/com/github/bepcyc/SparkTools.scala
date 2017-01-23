package com.github.bepcyc

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, SQLContext}

import scala.util.{Failure, Success, Try}

object SparkTools {

  implicit class DataFrameOps(df: DataFrame) {
    def createTable(name: String, partitions: Seq[String] = Seq.empty) =
      HiveTable(name)(df.sqlContext).createTableFrom(df, partitions)
  }

  case class HiveTable(tableName: String)(implicit val ctx: SQLContext) {

    import ctx.implicits._

    val (db, name): (String, String) = {
      val parts = tableName.split('.')
      (parts(0), parts(1))
    }
    lazy val exists: Boolean =
      ctx
        .sql(s"SHOW TABLES IN $db")
        .where($"tableName" === name)
        .count == 1

    def createTableFrom(df: DataFrame, partitions: Seq[String]): Unit =
      createTableFrom(df = df, partitions = Option(partitions) filter (_.nonEmpty))

    def createTableFrom(df: DataFrame,
                        partitions: Option[Seq[String]] = None): Unit = {
      if (!exists) {
        val query = CreateTable(df = df, tableName = tableName, partitions = partitions).sqlQuery
        ctx sql query
      }
    }

    /**
      * Aligns DataFrame's column and data order with a table, respecting partitions order
      *
      * @param df source DataFrame
      * @return DataFrame with aligned columns
      */
    def align(df: DataFrame,
              partitions: Option[Seq[String]] = None,
              sortWithinPartitions: Option[Seq[String]] = None): DataFrame = exists match {
      case false => df
      case true => val cols: Array[Column] = ctx.table(tableName).columns.map(df(_))
        val dfSelect = df.select(cols: _*)
        val dfPartitions = partitions orElse discoverPartitions match {
          case Some(parts) => dfSelect.repartition(parts.map(df(_)): _*)
          case _ => dfSelect
        }
        sortWithinPartitions match {
          case Some(parts) => dfPartitions.sortWithinPartitions(parts.map(dfPartitions(_)): _ *)
          case _ => dfPartitions
        }
    }

    def discoverPartitions: Option[Seq[String]] = exists match {
      case false => None
      case true => Try(ctx.sql(s"SHOW PARTITIONS $tableName").as[String].first) match {
        case Failure(_) => None
        case Success(str) => val parts = str
          .split('/')
          .filter(_.contains('='))
          .map(p => p.split('=').head)
          .toSeq
          Option(parts).filterNot(_.isEmpty)
      }
    }
  }

  trait HiveTableOperation {
    val partitions: Option[Seq[String]]

    def structFieldSQL(s: StructField): String = s"${s.name} ${s.dataType.simpleString}"

    def sqlQuery: String

    def fieldsAndPartitionsFrom(df: DataFrame): (Seq[String], Seq[String]) = {

      val (f, p) = {
        // we want to preserve the order of fields everywhere, so use of partition() is not possible
        val flds: Seq[StructField] = df
          .schema
          .filterNot(f => partitions.getOrElse(Seq.empty).toSet.contains(f.name))
        // will fail if no such partition exists - and it's good
        val prtns: Seq[StructField] = partitions.getOrElse(Seq.empty).map(p => df
          .schema.find(_.name == p).get)
        (flds, prtns)
      }
      (f map structFieldSQL, p map structFieldSQL)
    }

  }

  // TODO: finish or kill it
  case class InsertQuery(sinkTableName: String, partitions: Option[Seq[String]], sourceTableName: String = "temp1")
                        (implicit val sqlContext: SQLContext) extends HiveTableOperation {
    val (fieldNames, partitionNames) = fieldsAndPartitionsFrom(sqlContext.table(sinkTableName))

    private lazy val template: Seq[(String, Option[String])] = Seq(
      "INSERT INTO TABLE %s" -> Some(sinkTableName),
      "PARTITION (%s)" -> Option(partitionNames).filter(_.nonEmpty).map(_ mkString ",")
    )
    lazy val fields: Seq[String] =
      sqlContext.table(sinkTableName).schema.fieldNames.filterNot(partitions.getOrElse(Seq.empty).toSet).toSeq

    override def sqlQuery: String = ""
  }

  case class CreateTable(df: DataFrame,
                         tableName: String,
                         partitions: Option[Seq[String]] = None,
                         format: Option[String] = Some("orc"),
                         tblProperties: Option[Map[String, String]] = Some(Map("orc.compress" -> "ZLIB"))
                        ) extends HiveTableOperation {

    val (fieldNames, partitionNames) = fieldsAndPartitionsFrom(df)

    private lazy val template: Seq[(String, Option[String])] = Seq(
      "CREATE TABLE %s (" -> Some(tableName),
      "%s" -> Some(fieldNames.mkString(",")),
      ")" -> Some(""),
      "PARTITIONED BY (%s)" -> Option(partitionNames).filter(_.nonEmpty).map(_ mkString ","),
      "STORED AS %s" -> format,
      "TBLPROPERTIES(%s)" -> tblProperties.map(_.map { case (k, v) => s"""'$k'='$v'""" }.mkString(","))
    )

    override def sqlQuery: String =
      template
        .collect { case (k, Some(v)) => k format v }
        .mkString(" ")
        .trim
  }

}

