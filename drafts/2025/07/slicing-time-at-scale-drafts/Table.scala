package com.vitthalmirji.luminate.cperf.spark

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, Storage}
import com.vitthalmirji.luminate.cperf.constants.ItemShareConstants.GEO_REGION_CD
import com.vitthalmirji.luminate.cperf.constants.StringConstants.{EMPTY_STRING, FORWARD_SLASH, GCS_PREFIX}
import com.vitthalmirji.luminate.cperf.datetime.DateTimeHelpers.extractDate
import com.vitthalmirji.luminate.cperf.gcp.GcsAffectedBlobs
import com.vitthalmirji.luminate.cperf.gcp.GcsInterpolators.GcsInterpolator
import com.vitthalmirji.luminate.cperf.gcp.ObjectActions.BlobOperations
import com.vitthalmirji.luminate.cperf.spark.ETL.getScannedTableFromView
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future => scalaFuture}
import scala.util.{Failure, Success, Try}

/**
 * Singleton Object [[com.vitthalmirji.luminate.cperf.spark.Table]]
 * represents library functions used on Spark / Hive tables
 */

class ComputeAffectedBlobs(
    page: Page[Blob],
    before: Option[LocalDateTime],
    after: Option[LocalDateTime],
    operator: String
) extends Callable[ListBuffer[Blob]] {
  override def call(): ListBuffer[Blob] = {
    val affectedBlobs = ListBuffer.empty[Blob]
    page.getValues.forEach { blob =>
      val blobLastUpdated = blob.updatedTs(None)
      (before, after, operator) match {
        case (Some(b), None, _) => if (blobLastUpdated.isBefore(b)) affectedBlobs += blob
        case (None, Some(a), _) => if (blobLastUpdated.isAfter(a)) affectedBlobs += blob
        case (Some(b), Some(a), "OR") =>
          if (blobLastUpdated.isBefore(b) || blobLastUpdated.isAfter(a)) affectedBlobs += blob
        case (Some(b), Some(a), "AND") =>
          if (blobLastUpdated.isBefore(b) && blobLastUpdated.isAfter(a)) affectedBlobs += blob
        case _ => None
      }
    }
    affectedBlobs
  }
}

object Table {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
    * [[com.vitthalmirji.luminate.cperf.spark.Table#repairRefreshTable(java.lang.String)]]
    * function performs MSCK REPAIR TABLE and spark catalog refresh on dataframe
    * @param tableName name of dataframe to perform spark catalog refresh
    */
  def repairRefreshTable(tableName: String): Unit = {
    assert(SparkSession.getActiveSession.isDefined)
    val spark = SparkSession.getActiveSession.get
    Try {
      spark.sql(s"MSCK REPAIR TABLE $tableName")
      spark.catalog.refreshTable(tableName)
    } match {
      case Success(_) => logger.warn(s"Refresh table $tableName successful")
      case Failure(exception) =>
        val errorMessage = s"Error refreshing table $tableName : ${exception.getMessage}"
        logger.error(errorMessage)
        exception.printStackTrace()
      // throw new Throwable(errorMessage)
    }
  }

  /**
    * [[com.vitthalmirji.luminate.cperf.spark.Table#inferTypeGetSqlEquivalentString(java.lang.Object)]]
    * function returns Sql equivalent String quoted or unquoted
    * @param value as generic type
    * @tparam T generic type specified / identified at runtime
    * @return returns quoted or unquoted string to support SQL syntax
    */
  def inferTypeGetSqlEquivalentString[T](value: T): String =
    value match {
      case s: String   => s"'$s'"
      case d: DateTime => s"'${d.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))}'"
      case _           => value.toString
    }

  /**
    * [[com.vitthalmirji.luminate.cperf.spark.Table#deleteDfsLocation(java.lang.String)]]
    * function physically deletes any file system location
    * @param location path given as string
    */
  def deleteDfsLocation(location: String): Unit = {
    val tryDeleteDfs = Try {
      val path       = getHadoopFsPath(location)
      val fileSystem = getFileSystem(path)
      logger.warn("Attempting to Delete location: " + path.toString)
      if (fileSystem.exists(path)) {
        fileSystem.delete(path, true)
      } else {
        logger.warn("Path " + path.toString + " doesn't exist, skipping")
      }
    }
    tryDeleteDfs match {
      case Success(_) => logger.warn(s"Delete location successful - $location")
      case Failure(exception) =>
        val errorMessage = s"Error deleting DFS location $location Cause = ${exception.getMessage}"
        logger.error(errorMessage)
        exception.printStackTrace()
        throw new Throwable(errorMessage)
    }
  }

  /**
    * [[com.vitthalmirji.luminate.cperf.spark.Table#getHadoopFsPath(java.lang.String)]]
    * functions returns HadoopFS equivalent Path type of path given in String
    * @param path path given in String
    * @return returns Hadoop FS equivalent Path type of path given in String
    */
  private def getHadoopFsPath(path: String): Path = new Path(path)

  /**
    * [[com.vitthalmirji.luminate.cperf.spark.Table#getFileSystem(org.apache.hadoop.fs.Path)]]
    * function returns FileSystem
    * @param path path given in HadoopFS Path
    * @return returns FileSystem (HDFS / GCS / S3)
    */
  private def getFileSystem(path: Path): FileSystem = {
    assert(SparkSession.getActiveSession.isDefined)
    path.getFileSystem(SparkSession.getActiveSession.get.sparkContext.hadoopConfiguration)
  }

  /**
   * This function is deprecated and will be removed in further releases, please use function
   * [[com.vitthalmirji.luminate.cperf.spark.Table#getAffectedPartitions(java.lang.String, java.time.LocalDateTime, scala.Seq)]] instead.
   *
   * Overloaded function getAffectedPartitions
   * @param sourceView View name as String
   * @param since get affected partitions after this LocalDateTime
   * @param viewFilters Filters as Option[Map] for View
   * @return Array of Option of MultiTablePartition case class
   *         usage
   *         viewFilters = Option(Map("geo_region_cd"->Seq("MX","CA")))
   *         view  = "ww_chnl_perf_stg_qa.view_us_mx_ca"
   *         since = yearMonthDate24HrTs"2022-11-01 00:00:00"
   *         getAffectedPartitions(view,since,viewFilters)
   *         which returns
   *         Array(Some(MultiTablePartition(ww_chnl_perf_stg_qa.tableviewtest_mx,[Lscala.Option;@34c43638)),
   *         Some(MultiTablePartition(ww_chnl_perf_stg_qa.tableviewtest_ca,[Lscala.Option;@55fd5bae)))
   *
   *
   */
  @deprecated
  def getAffectedPartitions(
      sourceView: String,
      since: LocalDateTime,
      viewFilters: Option[Map[String, Seq[String]]] = None
  ): Array[MultiTablePartition] =
    if (viewFilters.isEmpty) {
      getAffectedPartitions(sourceView, since, Seq())
    } else {
      getAffectedPartitions(sourceView, since, viewFilters.get(GEO_REGION_CD))
    }

  /**
   * Overloaded function getAffectedPartitions
   *
   * @param sourceView  View name as String
   * @param since       get affected partitions after this LocalDateTime
   * @param geoRegionFilters Filters as Seq for View, use empty Seq() when no filters are to be applied
   * @return Array of Option of MultiTablePartition case class
   *         usage
   *         geoRegionFilters = Seq("MX","CA")
   *         view  = "ww_chnl_perf_stg_qa.view_us_mx_ca"
   *         since = yearMonthDate24HrTs"2022-11-01 00:00:00"
   *         getAffectedPartitions(view,since,viewFilters)
   *         which returns
   *         Array(Some(MultiTablePartition(ww_chnl_perf_stg_qa.tableviewtest_mx,[Lscala.Option;@34c43638)),
   *         Some(MultiTablePartition(ww_chnl_perf_stg_qa.tableviewtest_ca,[Lscala.Option;@55fd5bae)))
   *
   *
   */
  def getAffectedPartitions(
      sourceView: String,
      since: LocalDateTime,
      geoRegionFilters: Seq[String]
  ): Array[MultiTablePartition] = {

    val spark  = SparkSession.getActiveSession.get
    val viewDf = spark.table(sourceView)

    val tableSeq = getScannedTableFromView(viewDf, geoRegionFilters)
    val tablesAffectedPartitions =
      tableSeq.map(table => MultiTablePartition(table, getAffectedPartitions(table, since)))

    tablesAffectedPartitions.toArray
  }

  /**
   * [[com.vitthalmirji.luminate.cperf.spark.Table#getAffectedPartitions]]
   * Function returns list of affected partitions including nested partition
   *
   * @param sourceTable table name as String
   * @param since get affected partitions after this LocalDateTime
   * @return Returns an Array of Partition case class
   *         usage:
   *         getAffectedPartitions("ww_chnl_perf_app.chnl_perf_dc_item_dly", yearMonthDate24HrTs"2022-11-01 00:00:00")
   *         which returns
   *         Array(
   *         Some(Partition(Map(invt_dt -> 2022-10-15, op_cmpny_cd -> WMT-US))),
   *         Some(Partition(Map(invt_dt -> 2022-10-20, op_cmpny_cd -> WMT-US))), ...
   *         )
   *         --------------------
   *         getAffectedPartitions("us_fin_mumd_dl_rpt_secure.store_item_mumd_dly", yearMonthDate24HrTs"2022-11-01 00:00:00")
   *         Array(Some(Partition(Map(mumd_dt -> 2022-10-11, op_cmpny_cd -> WMT-US))), ...)
   *         --------------------
   *         getAffectedPartitionsOld("ww_chnl_perf_app.chnl_perf_store_item_dly", yearMonthDate24HrTs"2022-11-01 00:00:00")
   *         Array(Some(Partition(Map(bus_dt -> 2020-11-02, op_cmpny_cd -> WMT-US))), ...)
   */
  def getAffectedPartitions(
      sourceTable: String,
      since: LocalDateTime
  ): Array[Option[Partition]] = {

    val tableLocation = getTableLocation(sourceTable)

    val blobLocation = if (tableLocation.endsWith(FORWARD_SLASH)) {
      tableLocation
    } else { tableLocation.concat(FORWARD_SLASH) }

    val firstPage =
      blobs"$blobLocation" (
        Array(Storage.BlobListOption.currentDirectory())
      )

    val executionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    val futuresVector = GcsAffectedBlobs
      .processPages(
        firstPage,
        tableLocation.replace(GCS_PREFIX, EMPTY_STRING).concat(FORWARD_SLASH),
        Vector.empty,
        since,
        executionContext
      )

    val futures = scalaFuture.sequence(futuresVector)
    Await.result(futures, Duration.Inf)

    val affectedBlobs = futures.value.get.get.flatten.toArray.distinct

    executionContext.shutdown()
    affectedBlobs
  }

  def getAffectedFiles(
      location: String,
      since: LocalDateTime
  ): Array[Option[String]] = {

    val blobLocation = if (location.endsWith(FORWARD_SLASH)) {
      location
    } else { location.concat(FORWARD_SLASH) }

    val firstPage =
      blobs"$blobLocation" (
        Array(Storage.BlobListOption.currentDirectory())
      )

    val executionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    val futuresVector = GcsAffectedBlobs
      .processAllPages(
        firstPage,
        location.replace(GCS_PREFIX, EMPTY_STRING).concat(FORWARD_SLASH),
        Vector.empty,
        since,
        executionContext
      )

    val futures = scalaFuture.sequence(futuresVector)
    Await.result(futures, Duration.Inf)

    val affectedBlobs = futures.value.get.get.flatten.toArray.distinct

    executionContext.shutdown()
    affectedBlobs
  }

  /**
    * [[com.vitthalmirji.luminate.cperf.spark.Table#getTableLocation(java.lang.String)]]
    * function fetches Hive / Spark dataframe location; used mostly on external tables;
    * works for managed dataframe as well.
    * @param tableName name of dataframe to fetch location
    * @return Location of the dataframe as String
    * Usage - {{{
    * val tableLocation: String = getTableLocation(tableName = "test_db.office")
    * }}}
    */
  def getTableLocation(tableName: String): String = {
    assert(SparkSession.getActiveSession.isDefined)
    Try {
      SparkSession.getActiveSession.get
        .sql(s"DESC FORMATTED $tableName")
        .filter("col_name = 'Location'")
        .select("data_type")
        .take(1)(0)
        .getAs[String]("data_type")
    } match {
      case Success(value) => value
      case Failure(exception) =>
        val errorMessage =
          s"Error fetching dataframe location for dataframe $tableName: ${exception.getMessage}"
        logger.error(errorMessage)
        exception.printStackTrace()
        throw new Throwable(errorMessage)
    }
  }

  /**
   * @param bucket gcs bucket name as string
   * @param before get affected blobs before this LocalDateTime
   * @param after get affected blobs after this LocalDateTime
   * @param operator Default "OR". Either use "AND" or "OR" when you provide both before and after
   * @return List of Blob
   *         usage: getAffectedBlobs("9e43704ec30d085719761442e5a40a59c36d327106e096fddc00bf62943072", after=yearMonthDate24HrTs"2022-06-27 00:00:00")
   *         returns List(Blob{bucket=9e43704ec30d085719761442e5a40a59c36d327106e096fddc00bf62943072,...})
   */
  def getAffectedBlobs(
      bucket: String,
      before: Option[LocalDateTime] = None,
      after: Option[LocalDateTime],
      operator: String = "OR"
  ): List[Blob] = {
    val gcsBucket = bucket"$bucket"
    var page      = gcsBucket.list()

    val tasks = new util.ArrayList[ComputeAffectedBlobs]()
    do {
      tasks.add(new ComputeAffectedBlobs(page, before, after, operator))
      page = page.getNextPage
    } while (page != null)

    val workers: ExecutorService                     = Executors.newCachedThreadPool()
    val results: util.List[Future[ListBuffer[Blob]]] = workers.invokeAll(tasks)
    results.asScala.toList.flatMap(future => future.get())
  }

  /**
   * [[com.vitthalmirji.luminate.cperf.spark.Table.ImplicitTablePartitionActions]] is an
   * implicit class definition that provides 2 important functions
   * [[com.vitthalmirji.luminate.cperf.spark.Table.ImplicitTablePartitionActions#drop(java.lang.String)]] &
   * [[com.vitthalmirji.luminate.cperf.spark.Table.ImplicitTablePartitionActions#delete(java.lang.String)]]
   * to operate specially on Spark/Hive dataframe's Partition placeholders
   * This uses com.vitthalmirji.luminate.cperf.spark.Partition as Partition placeholders
   *
   * @param tablePartitions list of derived / defined com.vitthalmirji.luminate.cperf.spark.Partition
   */
  implicit class ImplicitTablePartitionActions(tablePartitions: List[Partition])
      extends Serializable {

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.Table.ImplicitTablePartitionActions#drop(java.lang.String)]]
     * function drops all listed partitions for dataframe given
     *
     * @param tableName name of dataframe for which listed Partitions must be dropped
     *                  Usage - {{{
     * val op_cmpny_cd = "WMT-US"
     * spark.sql("DROP TABLE IF EXISTS test_db.test_table_drop_partitions PURGE")
     * spark.sql("""
     *|CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table_drop_partitions (
     *|id INT) PARTITIONED BY(bus_dt STRING, op_cmpny_cd STRING, week_nbr INT) STORED AS ORC
     *|LOCATION 'spark-warehouse/test_db.db/test_table_drop_partitions'
     *|""".stripMargin)
     * val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq((1, "2021-01-01", op_cmpny_cd, 1),
     *(2, "2021-01-08", op_cmpny_cd, 2),
     *(3, "2021-01-16", "WMT.COM", 3),
     *(4, "2021-01-24", op_cmpny_cd, 4)))).toDF("id", "bus_dt", "op_cmpny_cd", "week_nbr")
     * df.write.mode("overwrite").insertInto("test_db.test_table_drop_partitions")
     *
     * val partitionsToDrop = List(
     *Partition(Map("bus_dt" -> "2021-01-01")),
     *Partition(Map("bus_dt" -> "2021-01-08")),
     *Partition(Map("bus_dt" -> "2021-01-24", "op_cmpny_cd" -> "WMT-US", "week_nbr"-> 4))
     *)
     *
     * partitionsToDrop.drop("test_db.test_table_drop_partitions")
     * }}}
     */
    def drop(tableName: String): Unit = {
      val partitionList = getPartitions(inferType = true)
      val partitionString =
        partitionList.map(partition => s"PARTITION(${partition.mkString(",")})").mkString(",")

      val query = s"ALTER TABLE $tableName DROP IF EXISTS $partitionString PURGE"
      logger.warn(s"Dropping partitions using query: $query")
      SparkSession.getActiveSession.foreach(_.sql(query).show())
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.Table.ImplicitTablePartitionActions#delete(java.lang.String)]]
     * function physically deletes the location of dataframe's given list of partitions
     *
     * @param tableName name of dataframe for which listed Partitions must be physically deleted
     *                  Usage - {{{
     * val op_cmpny_cd = "WMT-US"
     * spark.sql("DROP TABLE IF EXISTS test_db.test_table_drop_partitions PURGE")
     * spark.sql("""
     *|CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table_drop_partitions (
     *|id INT) PARTITIONED BY(bus_dt STRING, op_cmpny_cd STRING, week_nbr INT) STORED AS ORC
     *|LOCATION 'spark-warehouse/test_db.db/test_table_drop_partitions'
     *|""".stripMargin)
     * val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq((1, "2021-01-01", op_cmpny_cd, 1),
     *(2, "2021-01-08", op_cmpny_cd, 2),
     *(3, "2021-01-16", "WMT.COM", 3),
     *(4, "2021-01-24", op_cmpny_cd, 4)))).toDF("id", "bus_dt", "op_cmpny_cd", "week_nbr")
     * df.write.mode("overwrite").insertInto("test_db.test_table_drop_partitions")
     *
     * val partitionsToDrop = List(
     *Partition(Map("bus_dt" -> "2021-01-01")),
     *Partition(Map("bus_dt" -> "2021-01-08")),
     *Partition(Map("bus_dt" -> "2021-01-24", "op_cmpny_cd" -> "WMT-US", "week_nbr"-> 4))
     *)
     *
     * partitionsToDrop.delete("test_db.test_table_drop_partitions")
     * }}}
     */
    def delete(tableName: String): Unit = {
      val partitionList = getPartitions(inferType = false)
      logger.warn(s"Attempting to delete partitions $partitionList")
      val tableLocation = getTableLocation(tableName)
      val dfsLocations =
        partitionList.map(partition => tableLocation + "/" + partition.mkString("/"))
      dfsLocations.foreach(deleteDfsLocation)
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.Table.ImplicitTablePartitionActions#getPartitions(boolean)]]
     * returns List containing Set of Strings after iterating through derived tablePartitions
     *
     * @param inferType If type should be inferred or not before returning as plain string or SQL equivalent string
     * @return returns plain string or SQL equivalent string quoted / unquoted
     */
    private def getPartitions(inferType: Boolean): List[Set[String]] =
      tablePartitions.map(partition =>
        partition.partitions.map { case (field, value) =>
          if (inferType) s"$field=${inferTypeGetSqlEquivalentString(value)}" else s"$field=${value.toString}"
        }.toSet
      )
  }

  /**
   * [[com.vitthalmirji.luminate.cperf.spark.Table.TableDataframeActions]] Implicit class provides action on Table wrapped in dataframe
   * @param dataframe Table as whole or filtered using query wrapped into Dataframe
   */
  implicit class TableDataframeActions(dataframe: DataFrame) extends Serializable {

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.Table.TableDataframeActions#getAffectedPartitionDates(java.time.LocalDateTime, java.lang.String)]]
     * @param since     previous DateTime when this function was accessed or list all partitions that got impacted after (since) this DateTime
     * @param dateRegex By default yyyy-MM-dd; regex = \\d{4}-\\d{2}-\\d{2}, pass explicitly if other than default pattern
     * @return Returns list of date partitions of given dataframe that got affected after given since date
     *         Usage:
     *         val affectedDates = spark
     *         .sql("SELECT * FROM ww_chnl_perf_app.chnl_perf_item_fact_dly WHERE op_cmpny_cd = 'WMT-US'")
     *         .getAffectedPartitionDates(since = yearMonthDate24HrTs"2022-04-03 00:00:00")
     */
    def getAffectedPartitionDates(
        since: LocalDateTime,
        dateRegex: String = "\\d{4}-\\d{2}-\\d{2}"
    ): Array[(LocalDate, LocalDateTime)] = {
      val FILE_LOCATION = "file_location"
      val affectedDates = dataframe
        .select(input_file_name().alias(FILE_LOCATION))
        .distinct()
        .rdd
        .mapPartitions(partition =>
          partition.flatMap { field =>
            val location        = field.getAs[String]("file_location")
            val blobLocation    = blob"$location"
            val blobLastUpdated = blobLocation.updatedTs(None)
            if (blobLastUpdated.isAfter(since)) {
              Some((extractDate(location, dateRegex), blobLastUpdated))
            } else {
              None
            }
          }
        )
        .reduceByKey((v1, v2) => if (v1.isAfter(v2)) v1 else v2)
        .flatMap(t => t._1.map((_, t._2)))
        .distinct()
        .collect()
      affectedDates
    }
  }
}
