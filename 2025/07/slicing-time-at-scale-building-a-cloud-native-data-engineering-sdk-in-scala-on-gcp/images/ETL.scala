package com.vitthalmirji.luminate.cperf.spark

import com.vitthalmirji.archetype.core.utils.SparkImplicits.newStringEncoder
import com.vitthalmirji.luminate.cperf.audit.AuditOps.completedAuditRecord
import com.vitthalmirji.luminate.cperf.audit.{ AuditOps, AuditRecord }
import com.vitthalmirji.luminate.cperf.common.{ Email, Helpers }
import com.vitthalmirji.luminate.cperf.constants.AuditConstants.FAILED
import com.vitthalmirji.luminate.cperf.constants.DateTimeConstants.YEAR_MONTH_DATE_24HOUR_TS_FORMAT
import com.vitthalmirji.luminate.cperf.constants.StringConstants.{
  AUDIT_COLUMN_NAMES,
  AVRO,
  AVRO_INPUT_FORMAT,
  BUS_DT,
  COLUMNS_SEPARATOR_CHARACTER,
  COMMA,
  CONVERT,
  CSV,
  DATA_TYPE_COL,
  DATES,
  DATE_FORMAT,
  DELETE_FLAG,
  DELTA,
  DELTA_FORMAT,
  DOT,
  DOT_COM,
  DROP_COLUMNS,
  DV_DEL_IND,
  DYNAMIC,
  EMPTY_STRING,
  EXISTING_DF,
  FALSE,
  FORWARD_SLASH,
  FULL_OUTER,
  GSON,
  HASHCODE,
  HISTORY,
  HISTORY_BACKFILL,
  HIVE,
  HYPHEN,
  ICEBERG,
  INCREMENTAL,
  INPUT_FORMAT,
  INSERT_FLAG,
  JACKSON,
  JSON,
  JSON_SERDE,
  LAZY_SIMPLE_SERDE,
  LEFT,
  LEFT_ANTI,
  LOAD_TS,
  MD5_HASH_ID,
  MD5_HASH_SEPERATOR,
  MERGE,
  NBR_ONE,
  NEW_AUDIT_COLUMN_NAMES,
  NO_CHANGE_FLAG,
  NULL_VAL,
  OPENCSV_SERDE,
  OPENX_JSON,
  OPERATION,
  OPERATION_PARAMETERS,
  ORC,
  ORC_INPUT_FORMAT,
  OVERWRITE,
  OVERWRITE_SCHEMA,
  PARQUET,
  PARQUET_INPUT_FORMAT,
  PARTITIONBY,
  PART_TS,
  PROVIDER,
  REFRESH_TYPE,
  REGEX_SERDE,
  RELEASE,
  RESTATEMENT,
  RESTORE,
  ROW_TS,
  RUN_DATE,
  SERDE_LIBRARY,
  SPARK_CATALOG,
  SPLIT_COL,
  SQUARE_BRACES,
  SRC,
  STG,
  TIMESTAMP,
  TRUE,
  UNION_ALL,
  UPDATED_DF,
  UPDATE_FLAG,
  UPD_TS,
  US,
  USER_ID,
  VERSION,
  WMT,
  WRITE
}
import com.vitthalmirji.luminate.cperf.datetime.DateTimeHelpers.getDatesBetweenAs
import com.vitthalmirji.luminate.cperf.gcp.ObjectActions.parallelCopy
import com.vitthalmirji.luminate.cperf.schema.SchemaOps.{ getNewColumnsDf, validateSchemaEnv }
import com.vitthalmirji.luminate.cperf.spark.DataQuality.DataQualityActions
import com.vitthalmirji.luminate.cperf.spark.ETL.ETLDataframeActions
import com.vitthalmirji.luminate.cperf.spark.Table.{ getTableLocation, repairRefreshTable }
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{ CatalogTable, HiveTableRelation }
import org.apache.spark.sql.catalyst.expressions.XxHash64
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  concat,
  concat_ws,
  current_timestamp,
  date_format,
  input_file_name,
  lit,
  max,
  md5,
  min,
  to_date,
  to_timestamp,
  when
}
import org.apache.spark.sql.types.{ StringType, StructType }
import org.apache.spark.sql.{ Column, DataFrame, SaveMode, SparkSession }
import scopt.OptionParser

import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{ LocalDate, LocalDateTime }
import java.util.concurrent.Executors
import java.util.{ Calendar, Properties }
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.reflect.runtime.universe.{ typeOf, TypeTag }
import com.vitthalmirji.luminate.cperf.bigquery.Helper.throwExpection
import com.vitthalmirji.luminate.cperf.constants.{ BQConstants => BQ }
object ETL extends Serializable {

  @transient
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  var helpers: Helpers.type       = Helpers
  var auditOps: AuditOps.type     = AuditOps
  var trinoUtils: TrinoUtils.type = TrinoUtils

  var etl = new ETL

  /**
   * [[com.vitthalmirji.luminate.cperf.constants.StringConstants#getAuditColumnsWithNewValues()]] returns Map of String &
   * SparkSQL Column with new values for all audit columns
   *
   * @return
   */
  def getAuditColumnsWithNewValues: Map[String, Column] =
    Map(
      USER_ID -> lit(SparkSession.getActiveSession.get.sparkContext.sparkUser),
      LOAD_TS -> to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT)),
      UPD_TS  -> to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT)),
      DELTA   -> lit(INSERT_FLAG)
    )

  /**
   * @param workflowName Name of the workflow/pipeline
   * @return
   */

  def cliParser(workflowName: String): OptionParser[CommandLineConfigs] = {
    val parser =
      new scopt.OptionParser[CommandLineConfigs](workflowName) {
        opt[String]('w', "workflow")
          .required()
          .action((x, c) => c.copy(workflow = x))
          .text("Defines the workflow which will be triggered")
        opt[String]('r', "refreshType")
          .required()
          .action((x, c) => c.copy(refreshType = x))
          .text("Defines refresh type of the pipeline")
        opt[Boolean]('q', "dqEnabled")
          .optional()
          .action((x, c) => c.copy(dqEnabled = x))
          .text("[Optional] To disable DQ, [default] enable")
        opt[String]('r', "runMode")
          .required()
          .action((x, c) => c.copy(runMode = x))
          .text("Defines the run mode of the pipeline: GLOBAL, LOCAL")
        opt[String]('t', "tenant")
          .optional()
          .action((x, c) => c.copy(tenant = Some(x)))
          .validate(x =>
            if (Seq("WMT", "WMT.COM", "SAMS", "SAMS.COM").contains(x)) {
              success
            } else { failure("Tenant should have values in (WMT,WMT.COM,SAMS,SAMS.COM)") }
          )
          .text("Defines the tenant for the run")
        opt[String]('r', "region")
          .optional()
          .action((x, c) => c.copy(region = Some(x)))
          .validate(x =>
            if (
              Seq("US", "MX", "CA", "K1", "K2", "K3", "CN", "AR", "BR", "GB", "IN", "WW")
                .contains(x)
            ) {
              success
            } else {
              failure("Region should have values in (US,MX,CA,K1,K2,K3,CN,AR,BR,IN,GB,WW)")
            }
          )
          .text("Defines the region for the run")
        opt[Calendar]('s', "startDate")
          .optional()
          .action((x, c) =>
            c.copy(startDate =
              LocalDateTime.ofInstant(x.toInstant, x.getTimeZone.toZoneId).toLocalDate
            )
          )
          .validate(x =>
            if (!x.before(Calendar.getInstance()) && !x.equals(Calendar.getInstance())) {
              failure("Option --startDate should not be greater than current date")
            } else {
              success
            }
          )
          .text("[Optional] Defines the start date to run the pipeline in yyyy-mm-dd format")
        opt[Calendar]('e', "endDate")
          .optional()
          .action((x, c) =>
            c.copy(endDate =
              LocalDateTime.ofInstant(x.toInstant, x.getTimeZone.toZoneId).toLocalDate
            )
          )
          .validate(x =>
            if (!x.before(Calendar.getInstance()) && !x.equals(Calendar.getInstance())) {
              failure("Option --endDate should not be greater than current date")
            } else {
              success
            }
          )
          .text("[Optional] Defines the end date to run the pipeline in yyyy-mm-dd format")
        opt[Calendar]('r', "runDate")
          .optional()
          .action((x, c) =>
            c.copy(runDate =
              LocalDateTime.ofInstant(x.toInstant, x.getTimeZone.toZoneId).toLocalDate
            )
          )
          .text("[Optional] Defines the run date of the pipeline in yyyy-mm-dd format")
        opt[String]('g', "globalDuns")
          .optional()
          .validate(x =>
            if (x == null || x.isEmpty) {
              failure("Option --globalDuns cannot be empty")
            } else if (x.endsWith(COMMA) || x.startsWith(COMMA)) {
              failure("Option --globalDuns cannot have empty value in given comma separated values")
            } else {
              success
            }
          )
          .action((x, c) => c.copy(globalDuns = Some(x).map(_.trim.split(COMMA))))
          .text(
            "[Optional] Defines the list of parent-company globalDuns for which job must be run"
          )
        opt[String]('n', "feedName")
          .optional()
          .validate(x =>
            if (x == null || x.isEmpty) {
              failure("Option --feedName cannot be empty")
            } else {
              success
            }
          )
          .action((x, c) => c.copy(feedName = Some(x)))
          .text(
            "[Optional] Defines the feed name for which job must be run"
          )
        opt[String]('f', "fileFormats")
          .optional()
          .validate(x =>
            if (x == null || x.isEmpty) {
              failure("Option --fileFormat cannot be empty")
            } else if (x.endsWith(COMMA) || x.startsWith(COMMA)) {
              failure("Option --fileFormat cannot have empty value in given comma separated values")
            } else {
              success
            }
          )
          .action((x, c) => c.copy(fileFormats = Some(x).map(_.trim.split(COMMA))))
          .text(
            "[Optional] Defines the file format for which job must be run"
          )
        opt[String]('s', "supplierNames")
          .optional()
          .validate(x =>
            if (x == null || x.isEmpty) {
              failure("Option --supplierName cannot be empty")
            } else if (x.endsWith(COMMA) || x.startsWith(COMMA)) {
              failure(
                "Option --supplierName cannot have empty value in given comma separated values"
              )
            } else {
              success
            }
          )
          .action((x, c) => c.copy(supplierNames = Some(x).map(_.trim.split(COMMA))))
          .text(
            "[Optional] Defines the supplier Name for which job must be run"
          )
        opt[Boolean]('o', "overwriteData")
          .optional()
          .action((x, c) => c.copy(overwriteData = Some(x)))
          .text(
            "[Optional] Defines the overwrite data flag for which job must be run"
          )
        help("help").text("prints this usage text")
      }
    parser
  }

  /**
   * Performs validation based on the dates provided in program arguments
   *
   * @param config Command line config which contains values for run, start and end dates
   * @return Boolean to indicate whether dates are valid
   */
  def validateDates(config: CommandLineConfigs): Boolean =
    if (
      config.startDate != null && config.endDate != null
      && config.startDate.isAfter(config.endDate)
    ) {
      logger.error("start date is after end date")
      false
    } else if (config.startDate == null && config.endDate != null) {
      logger.error("only end date provided, please provide start date as well, or neither")
      false
    } else {
      true
    }

  /**
   * Performs validation of refresh type
   * @param config Command line config which contains refresh type
   * @param validRefreshTypes Valid refresh types
   * @return Boolean to indicate whether refresh type is valid
   */
  def validateRefreshType(config: CommandLineConfigs, validRefreshTypes: Seq[String]): Boolean =
    if (!validRefreshTypes.contains(config.refreshType)) {
      logger.error(
        s"Specified refresh type is invalid. Valid refresh types : ${validRefreshTypes.mkString(COMMA)}".stripMargin
      )
      false
    } else {
      true
    }

  /**
   * Updates the audit record with values corresponding to failure and exits the system
   * @param errMsg Error message to be logged and updated to audit record
   * @param auditRecord Audit Record to be updated
   * @param auditDbProperties Audit database properties
   */
  def updateFailedAuditAndExit(
      errMsg: String,
      auditRecord: AuditRecord,
      auditDbProperties: Properties
  ): Unit = {
    logger.error(errMsg)
    val updatedAuditRecord = completedAuditRecord(auditRecord).copy(
      status = FAILED,
      errorMsg = Some(errMsg)
    )
    auditOps.updateAuditRecord(updatedAuditRecord, auditDbProperties)
    helpers.systemExit(1)
  }

  /**
   * Finds the range of dates for given table using given date column and returns a list
   * of all dates in between
   *
   * @param tableName  Name of the table
   * @param dateColumn Column to be used to find date range
   * @return Set of dates
   */
  def getDateRangeFromTable(tableName: String, dateColumn: String): Set[String] = {
    val spark = SparkSession.getActiveSession.get
    val minMaxDates =
      spark.read.table(tableName).agg(min(dateColumn), max(dateColumn)).head()
    val startDate = minMaxDates.getDate(0).toLocalDate
    val endDate   = minMaxDates.getDate(1).toLocalDate
    getDatesBetweenAs[String](startDate, endDate)
  }

  /**
   * Method to merge the incoming dataframe with current target table.
   * @param schemas       Schemas object which has table names
   * @param transformedDf The transformed dataframe coming from transformer post transformation with newly calculated delta flags
   * @param primaryKeys   Primary keys for the target table
   * @param datesDf       processed dates dataframe
   * @param dateCol       DatePartition Column name
   */
  def deltaMerge(
      schemas: SchemasTrait,
      transformedDf: DataFrame,
      primaryKeys: Seq[String],
      datesDf: Option[DataFrame] = None,
      dateCol: String = BUS_DT
  ): Unit = {

    val finalTransformedDf =
      transformedDf.filter(col(DELTA).isin(INSERT_FLAG, UPDATE_FLAG, DELETE_FLAG))

    val mergeExpr = primaryKeys.foldLeft(lit(TRUE))((exprResult, column) =>
      col(s"$EXISTING_DF.$column") === col(s"$UPDATED_DF.$column") && exprResult
    )

    val mergeCondition: Column = datesDf.map { df =>
      val datesList = df
        .select(col(dateCol).cast(StringType))
        .map(row => row.getAs[String](dateCol))
        .collect()
      col(s"$EXISTING_DF.$dateCol").isin(datesList: _*) && mergeExpr
    }.getOrElse(mergeExpr)

    val expression: Map[String, String] = finalTransformedDf.columns
      .filterNot(Seq(DELTA, LOAD_TS, UPD_TS).contains(_))
      .flatMap(key => Map(s"$EXISTING_DF.$key" -> s"$UPDATED_DF.$key"))
      .toMap

    val currentTimestamp = current_timestamp.toString()

    val updateExpression = expression ++ Map(s"$EXISTING_DF.$UPD_TS" -> currentTimestamp)

    val insertExpression = expression ++
      Map(s"$EXISTING_DF.$LOAD_TS" -> currentTimestamp, s"$EXISTING_DF.$UPD_TS" -> currentTimestamp)

    DeltaTable
      .forName(schemas.getTargetTable)
      .as(EXISTING_DF)
      .merge(finalTransformedDf.as(UPDATED_DF), mergeCondition)
      .whenMatched(condition = s"$UPDATED_DF.$DELTA = '$DELETE_FLAG'")
      .delete()
      .whenMatched(condition = s"$UPDATED_DF.$DELTA = '$UPDATE_FLAG'")
      .updateExpr(updateExpression)
      .whenNotMatched(condition = s"$UPDATED_DF.$DELTA = '$INSERT_FLAG'")
      .insertExpr(insertExpression)
      .execute()
  }

  /**
   * This method parallely validates tables columns names and it's ordering with exisiting view Columns
   * @param table
   * @param viewResolvedSchema
   * @param executionContext
   * @return Future of Boolean
   */
  def validateSchema(
      table: String,
      viewResolvedSchema: StructType,
      executionContext: ExecutionContext
  ): Future[Boolean] =
    Future {
      val spark = SparkSession.getActiveSession.get

      val tableSchema = spark.table(table).schema

      viewResolvedSchema.equals(tableSchema)

    }(executionContext)

  /**
   *  Use Case : INTL Automatic View DDL Creation
   *  This method creates view DDL based on schema and view passed checking catalog with tables of the schema pattern
   *  which could be source tables of the view.
   *  In case of Delta table, this method also supports creation of the corresponding view in Trino, to enable reading of Delta
   *  view from Data Discovery.
   *
   * @param tgtViewSchema Schema name for View
   * @param tgtViewName Name of the view
   * @param tableSchema Schema name for Table
   * @param createViewInTrino Boolean to indicate whether corresponding view should be created in Trino,
   *                    applicable only in case of Delta tables
   * @param trinoProperties Connection properties for Trino, if properties are not provided and createViewInTrino
   *                        is set to true, trino properties are fetched from CCM Service ID CPERF-DATAPIPELINE-UTILITIES.
   *                        The properties object should include below properties -
   *                           val trinoProperties = new Properties()
   *                           trinoProperties.setProperty("url", "url")
   *                           trinoProperties.setProperty("user", "user")
   *                           trinoProperties.setProperty("password", "pwd")
   *                           trinoProperties.setProperty("SSL", "true")
   *                           trinoProperties.setProperty("SSLTrustStorePath", "trust_store_path")
   *                           trinoProperties.setProperty("SSLTrustStorePassword", "trust_store_pwd")
   * @return view DDL String
   *
   * Usage:
   * for eg : tgtViewSchema passed = ww_chnl_perf_app_vm , tgtViewName = chnl_perf_item_snapshot , tableSchema = us_chnl_perf_app
   * it creates the DDL based on the new tables schema passed and returns the DDL.
   *
   */

  def createViewDDL(
      tgtViewSchema: String,
      tgtViewName: String,
      tableSchema: String,
      createViewInTrino: Boolean = true,
      trinoProperties: Option[Properties] = None
  ): String = {
    if (tgtViewSchema.contains("qa") != tableSchema.contains("qa")) {
      val errMsg = s"Table and view schema environment are not matching!!"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }
    val spark          = SparkSession.getActiveSession.get
    val tableToOnboard = s"$tableSchema.$tgtViewName"
    val viewName       = s"$tgtViewSchema.$tgtViewName"
    val viewExists     = spark.catalog.tableExists(viewName)

    val sourceTable = spark.catalog.tableExists(tableToOnboard)

    sourceTable match {
      case TRUE =>
        val provider = spark
          .sql(s"describe formatted $tableToOnboard")
          .filter(col("col_name") === "Provider")
          .select(DATA_TYPE_COL)
          .collect()(0)(0)
          .toString
        if (provider.equalsIgnoreCase("hive")) {
          spark.conf.unset("spark.sql.catalog.spark_catalog")
        }
        viewExists match {
          case TRUE =>
            val viewDf             = spark.table(viewName)
            val resolvedCols       = viewDf.schema.map(c => c.name)
            val existingViewTables = getScannedTableFromView(viewDf)
            val executionContext =
              ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
            val futures     = validateSchema(tableToOnboard, viewDf.schema, executionContext)
            val schemaCheck = Await.result(futures, Duration.Inf)
            if (schemaCheck) {
              val allTables = (existingViewTables :+ tableToOnboard).distinct
              val viewSrcQuery = allTables
                .flatMap(f => Seq(s"Select ${resolvedCols.mkString(COMMA)} from $f"))
                .mkString(UNION_ALL)
              val noDefinitionChange = existingViewTables.contains(tableToOnboard)

              val viewDDL = if (noDefinitionChange) {
                s"""create view if not exists $viewName as
                   |(
                   |$viewSrcQuery
                   |)""".stripMargin
              } else {
                s"""create or replace view $viewName as
                   |(
                   |$viewSrcQuery
                   |)""".stripMargin
              }

              if (createViewInTrino && provider.equalsIgnoreCase(DELTA_FORMAT)) {
                trinoUtils.createViewInTrino(
                  tgtViewSchema,
                  tgtViewName,
                  viewSrcQuery,
                  viewExistsInMetastore = TRUE,
                  !noDefinitionChange,
                  trinoProperties
                )
              }

              viewDDL
            } else {
              val errMsg = s"Table and view schema not matching!!"
              logger.error(errMsg)
              throw new Exception(errMsg)
            }
          case FALSE =>
            val resolvedCols = spark.table(tableToOnboard).schema.map(c => c.name)
            val viewSrcQuery = s"select ${resolvedCols.mkString(COMMA)} from $tableToOnboard"
            val viewDDL =
              s"""create view if not exists $viewName as
               |(
               |$viewSrcQuery
               |)""".stripMargin

            if (createViewInTrino && provider.equalsIgnoreCase(DELTA_FORMAT)) {
              trinoUtils.createViewInTrino(
                tgtViewSchema,
                tgtViewName,
                viewSrcQuery,
                viewExistsInMetastore = FALSE,
                definitionChange = TRUE,
                trinoProperties
              )
            }

            viewDDL
          case _ =>
            val errorMsg = "Invalid boolean value"
            logger.error(errorMsg)
            throw new IllegalArgumentException(errorMsg)
        }
      case FALSE =>
        val errMsg = s"Table for the region is not present in catalog!!!"
        logger.error(errMsg)
        throw new Exception(errMsg)
      case _ =>
        val errorMsg = "Invalid boolean value"
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
    }
  }

  /**
   * Function returns underlying scanned tables based on filter condition in dataframe
   * @param df Dataframe created from query / view
   * @param geoRegionFilters Geo region filter to be applied on dataframe
   * @return Tables sequence as strings
   */
  def getScannedTableFromView(df: DataFrame, geoRegionFilters: Seq[String] = Seq()): Seq[String] = {
    val tables = df.queryExecution.sparkPlan.collect { case tableNames: FileSourceScanExec =>
      tableNames.tableIdentifier.map(f => f.unquotedString)
    }
      .filterNot(_.isEmpty)
      .map(_.get)
      .map(_.replace(SPARK_CATALOG + DOT, EMPTY_STRING))
      .distinct

    geoRegionFilters.nonEmpty match {
      case TRUE =>
        tables.par
          .filter(tableName =>
            geoRegionFilters.exists(geoRegion =>
              tableName.toLowerCase.startsWith(geoRegion.toLowerCase)
            )
          )
          .seq
      case FALSE => tables
    }
  }

  /**
   * [[com.vitthalmirji.luminate.cperf.spark.ETL.ETLDataframeActions#getTableDetail(String,String)]] fetches hive table specific details
   *
   * @return Returns specific hive table description details
   * @param table       defines the hive table
   * @param tableDetail defines the specific table description
   */
  def getTableDetail(table: String, tableDetail: String): String = {
    val spark = SparkSession.active
    spark
      .sql(s"describe formatted $table")
      .filter(s"col_name = '$tableDetail'")
      .selectExpr(DATA_TYPE_COL)
      .distinct()
      .collect()
      .map(row => row.getAs[String](DATA_TYPE_COL))
      .mkString(COMMA)
  }

  /**
   *
   * @param colSeq Column which defines rowTs or partTs
   * @param tableName Processed dates table name
   * @param currRefreshType Current refresh type for run
   * @param refreshTypeCol Refresh type Column name
   * @param datesFilter Dates in batch to be filtered
   * @param dateCol  Dates Column name
   * @param datePartitionCol Partition Column for Dates table with default value
   * @param schema  Optional parameter for schema trait
   * @return Option[Timestamp]
   *
   */
  def getMinRowPartTsFromPrcsdDates(
      tableName: String,
      currRefreshType: String,
      datesFilter: Seq[String],
      colSeq: Seq[String] = Seq("row_ts", "part_ts"),
      refreshTypeCol: String = "refresh_type",
      dateCol: String = "dates",
      datePartitionCol: String = RUN_DATE,
      schema: Option[SchemasTrait] = None
  ): Option[Array[(Timestamp, Timestamp)]] = {
    val spark = SparkSession.getActiveSession.get
    if (!spark.catalog.tableExists(tableName)) throw new Exception(s"$tableName doesn't exist")

    val schemaSeq = if (schema.isEmpty || (schema.isDefined && schema.get.newDatesTablePerDay)) {
      colSeq :+ dateCol :+ refreshTypeCol
    } else {
      colSeq :+ dateCol :+ refreshTypeCol :+ datePartitionCol
    }

    val df = spark.table(tableName)

    if (!schemaSeq.toSet.forall(df.columns.toSet.contains)) {
      None
    } else {

      val filterDf = if (schema.isEmpty || (schema.isDefined && schema.get.newDatesTablePerDay)) {
        df.filter(col(refreshTypeCol) === currRefreshType && col(dateCol).isin(datesFilter: _*))
      } else {
        df.filter(
          col(
            datePartitionCol
          ).isin(schema.get.cliConfigs.runDate.toString) && col(
            refreshTypeCol
          ) === currRefreshType && col(dateCol).isin(datesFilter: _*)
        )
      }
      if (!filterDf.isEmpty) {
        Some(
          filterDf
            .agg(min(colSeq(0)), min(colSeq(1)))
            .rdd
            .map(x => (x.getTimestamp(0), x.getTimestamp(1)))
            .collect()
        )
      } else {
        None
      }
    }
  }

  /**
   * Take backup of target table for given dates by copying files from target to backup location
   * Backup dates table is used to keep track of the dates for which backup is
   * already done, it helps in avoiding taking backup of the same dates twice,
   * otherwise the original backup data could be overwritten by restated data
   * @param schemas Schemas object which has table names
   * @param backupDatesDf Dates for which data needs to be backed up
   * @param backupConfig Backup config which contains the column names used in backup
   * @return Boolean to indicate whether backup was successful
   */
  def performTargetBackupForDates(
      schemas: SchemasTrait,
      backupDatesDf: DataFrame,
      backupConfig: BackupConfig
  ): Boolean = {

    val spark           = SparkSession.active
    val backedUpDatesDf = spark.table(schemas.getBackupDatesTable)

    val dfDatesCol          = backupConfig.dfDatesCol
    val tableDatesCol       = backupConfig.tableDatesCol
    val dfRefreshTypeCol    = backupConfig.dfRefreshTypeCol
    val tableRefreshTypeCol = backupConfig.tableRefreshTypeCol
    val rowTsCol            = backupConfig.rowTsCol
    val partTsCol           = backupConfig.partTsCol
    val datePartCol         = backupConfig.datePartCol

    val effectiveDatesDf = backupDatesDf
      .join(
        backedUpDatesDf,
        backupDatesDf.col(dfDatesCol) === backedUpDatesDf(tableDatesCol)
          && backupDatesDf.col(dfRefreshTypeCol) === backedUpDatesDf(tableRefreshTypeCol),
        LEFT_ANTI
      )
      .withColumn(rowTsCol, lit(NULL_VAL))
      .withColumn(partTsCol, lit(NULL_VAL))

    if (!effectiveDatesDf.isEmpty) {

      val backupLocation = getTableLocation(schemas.getBackupTable)

      val prevDf = spark
        .table(schemas.getTargetTable)
        .filter(
          col(datePartCol)
            .isin(effectiveDatesDf.select(dfDatesCol).map(x => x.getString(0)).collect(): _*)
        )

      logger.info(s"Performing backup into $backupLocation")
      val backupStatus = etl.executeBackup(prevDf, backupLocation)

      if (backupStatus) {

        effectiveDatesDf
          .select(dfDatesCol, dfRefreshTypeCol, rowTsCol, partTsCol)
          .coalesce(1)
          .write
          .format(PARQUET)
          .mode(SaveMode.Append)
          .insertInto(schemas.getBackupDatesTable)

        logger.info("Backup creation completed. Refreshing tables")
        repairRefreshTable(schemas.getBackupTable)
        true

      } else {
        logger.error(s"Failed to perform backup into $backupLocation")
        false
      }
    } else {
      logger.info("Backup is already complete for given dates")
      true
    }
  }

  /**
   * Take backup of entire target table by copying files from target to backup table
   * Backup check table is created on successful backup of table and it's presence is used
   * to detect if backup is already complete for a table
   * @param schemas Schemas object which has table names
   * @return Boolean to indicate whether backup was successful
   */
  def performTargetBackup(
      schemas: SchemasTrait
  ): Boolean = {
    val spark = SparkSession.active
    if (!spark.catalog.tableExists(schemas.getBackupCheckTable)) {
      val backupLocation = getTableLocation(schemas.getBackupTable)
      val prevDf         = spark.table(schemas.getTargetTable)
      logger.info(s"Performing backup into $backupLocation")
      val backupStatus = etl.executeBackup(prevDf, backupLocation)
      if (backupStatus) {
        prepareExecution(
          List(
            getSchema(
              schemas.datesSchema,
              schemas.stgSchema,
              schemas.getBackupCheckTableName
            )
          )
        )
        logger.info("Backup creation completed. Refreshing tables")
        repairRefreshTable(schemas.getBackupTable)
      } else {
        logger.error(s"Failed to perform backup into $backupLocation")
      }
      backupStatus
    } else {
      logger.info("Backup is already completed for given dates")
      true
    }
  }

  /**
   * @param listOfSchemas List of schemas to be created in the DB before pipeline execution
   */

  def prepareExecution(listOfSchemas: List[String]): Unit =
    listOfSchemas.foreach { schema =>
      SparkSession.getActiveSession.get.sql(schema)
    }

  /**
   * @param ddl DDL to be executed
   * @param schema Schema to be used for DDL execution
   * @param tablename Tablename for DDL execution
   * @return
   */

  def getSchema(ddl: String, schema: String, tablename: String): String =
    ddl.replace("{SCHEMA}", schema).replace("{TABLENAME}", tablename)

  /**
   * Performs Data Quality checks and return the status of DQ
   * @param schemas Schemas object which has table names
   * @param finalDf Dataframe on which data quality checks have to be performed
   * @param ruleSetId rule set id for DQ
   * @param email Email details to send DQ report
   * @param dbProps Database details to store DQ result
   * @param batchId Current batch ID to set in DQ table, when dbProps is provided
   * @param emailOnSuccess Boolean to indicate whether email should be sent on DQ success when
   *                       dbProps is provided, email will always be sent in case of failure,
   *                       if emails details are provided
   * @return Boolean to indicate whether DQ was successful
   */
  def performDq(
      schemas: SchemasTrait,
      finalDf: DataFrame,
      ruleSetId: Int,
      email: Option[Email] = None,
      dbProps: Option[Properties] = None,
      batchId: Long = 0,
      emailOnSuccess: Boolean = false
  ): Boolean =
    try {
      validateSchemaEnv(schemas)
      if (schemas.cliConfigs.dqEnabled && !finalDf.isEmpty) {
        if (dbProps.isDefined) {
          finalDf.executeDQWithDBWrite(
            ruleSetId = ruleSetId,
            emailDetails = email,
            emailOnSuccess = emailOnSuccess,
            schemas = schemas,
            dbProps = dbProps.get,
            batchId = batchId
          )
        } else {
          finalDf.executeDQ(
            ruleSetId,
            email,
            tableName = Some(schemas.getTargetTable)
          )
        }
      } else {
        true
      }
    } catch {
      case t: Throwable =>
        logger.error(s"Error executing DQ ${t.getMessage} ${t.toString}")
        t.printStackTrace()
        throw t
    }

  /**
   * Returns the last processed row timestamp for a dataframe as the UPD_TS for updated rows,
   * current timestamp if no rows were updated or None if dataframe is empty
   * @param finalDf Dataframe for which row ts needs to be returned
   * @return Row timestamp for given dataframe
   */
  def getLastPrcsdRowTs(finalDf: DataFrame): Timestamp =
    if (!finalDf.filter(col(DELTA) =!= NO_CHANGE_FLAG).isEmpty) {
      finalDf
        .filter(col(DELTA) =!= NO_CHANGE_FLAG)
        .select(UPD_TS)
        .head
        .getTimestamp(0)
    } else {
      Timestamp.valueOf(LocalDateTime.now())
    }

  /**
   * Filters out the dates that are already processed (present in the processed dates table) from
   * the given list of dates for specified refresh type
   * @param schema Schemas object which has table names
   * @param datesList Dates to be processed
   * @param datesCol Dates column name in processed dates table
   * @param refreshTypeCol Refresh type column name in processed dates table
   * @param datePartitionCol Dates column name on which processed date is partitioned,
   * applicable only when [[com.vitthalmirji.luminate.cperf.spark.SchemasTrait#datesTablePerDay()]]
   * is set to false
   * @return List of dates after removing already processed dates
   */
  def removeProcessedDates(
      schema: SchemasTrait,
      datesList: Seq[String],
      datesCol: String = DATES,
      refreshTypeCol: String = REFRESH_TYPE,
      datePartitionCol: String = RUN_DATE
  ): Seq[String] = {

    val config = schema.cliConfigs
    val spark  = SparkSession.active

    val processedTable = if (config.refreshType.equalsIgnoreCase(HISTORY_BACKFILL)) {
      schema.getBackfillDatesTable
    } else {
      schema.getPrcsdDatesTable
    }

    if (spark.catalog.tableExists(processedTable)) {

      val processedDatesTable = if (schema.newDatesTablePerDay) {
        spark
          .table(processedTable)
      } else {
        spark
          .table(processedTable)
          .filter(col(datePartitionCol) === lit(schema.cliConfigs.runDate.toString))
      }

      val processedDates = processedDatesTable
        .filter(col(refreshTypeCol) === config.refreshType)
        .select(datesCol)
        .map(x => x.getAs[String](datesCol))
        .collect
        .toSeq

      val duplicateDates = datesList.intersect(processedDates)

      if (duplicateDates.nonEmpty) {
        logger.info(s"Found ${duplicateDates.length} duplicate dates. Removing them.....")
      }
      datesList.diff(processedDates)
    } else {
      datesList
    }
  }

  /**
   * Derives list of dates that need to be processed based on the refresh type
   * @param schema Schemas object which has table names
   * @param historyRange Number of days to be considered for history run
   * @param datePartitionCol Date partition column name in target table
   * @param getAffectedPartitions Get affected partitions implementation for restatement
   * @return List of dates that need to be processed
   */
  def getListOfDates(
      schema: SchemasTrait,
      historyRange: Int = 364 * 2,
      datePartitionCol: String = BUS_DT,
      getAffectedPartitions: Option[CommandLineConfigs => Set[String]] = None
  ): Seq[String] = {

    val config = schema.cliConfigs

    val datesList: Set[String] = config.refreshType match {

      case INCREMENTAL =>
        val date = config.runDate.minusDays(1)
        Set(date.format(DateTimeFormatter.ofPattern(DATE_FORMAT)))

      case HISTORY =>
        val (startDate, endDate) = if (config.startDate != null && config.endDate != null) {
          (config.startDate, config.endDate)
        } else if (config.startDate != null) {
          (config.startDate, config.runDate.minusDays(1))
        } else {
          val endDate   = config.runDate.minusDays(1)
          val startDate = endDate.minusDays(historyRange)
          (startDate, endDate)
        }
        getDatesBetweenAs[String](startDate, endDate)

      case RESTATEMENT =>
        if (config.startDate != null && config.endDate != null) {
          getDatesBetweenAs[String](config.startDate, config.endDate)
        } else if (config.startDate != null) {
          getDatesBetweenAs[String](config.startDate, config.runDate.minusDays(1))
        } else if (getAffectedPartitions.isEmpty) {
          logger.error("Affected partitions implementation is missing, returning no dates")
          Set()
        } else {
          getAffectedPartitions.get(config)
        }

      case HISTORY_BACKFILL =>
        val tableToRead = if (!getTableIfExists(schema.getTargetTable).isEmpty) {
          Some(schema.getTargetTable)
        } else if (!getTableIfExists(schema.getBackfillTable).isEmpty) {
          Some(schema.getBackfillTable)
        } else {
          None
        }
        val spark = SparkSession.getActiveSession.get

        if (tableToRead.nonEmpty) {
          spark
            .table(tableToRead.get)
            .select(datePartitionCol)
            .distinct()
            .collect()
            .map(x => x.get(0).toString)
            .toSet

        } else {
          Set()
        }
    }
    datesList.toSeq.sorted.reverse.toIndexedSeq
  }

  /**
   * Returns a table if it exists, otherwise empty dataframe
   * @param tableName Table that is to be returned
   * @return Dataframe
   */
  def getTableIfExists(tableName: String): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    if (spark.catalog.tableExists(tableName)) {
      spark.read
        .table(tableName)
    } else {
      spark.emptyDataFrame
    }
  }

  /**
   * Method to merge the incoming dataframe with added/removed columns with current target table snapshot.
   *
   * @param transformedDf   the transformed dataframe coming from transformer post transformation
   * @param primaryKeys     Primary keys for the target table
   * @param selectCols      Columns to be selected in order
   * @param schemas         schemas Trait
   * @param datesDf         processed dates dataframe
   * @param dateCol         DatePartition Column name
   * @return
   */
  @deprecated
  def getEvolvedBackfillDf(
      transformedDf: DataFrame,
      primaryKeys: Seq[String],
      selectCols: Seq[String],
      schemas: SchemasTrait,
      datesDf: Option[DataFrame] = None,
      dateCol: String = BUS_DT
  ): DataFrame = {

    val spark: SparkSession = SparkSession.getActiveSession.get

    val version = getPrevWriteVersion(schemas.getTargetTable, schemas.cliConfigs.runDate)
    val tgtDF   = spark.sql(s"Select * from ${schemas.getTargetTable} VERSION as of $version")

    val finalTargetDf: DataFrame = if (datesDf.isDefined) {
      val finalDatesList = datesDf.get
        .select(col(dateCol).cast(StringType).alias(dateCol))
        .map(row => row.getAs[String](dateCol))
        .collect()
      logger.info(s"final dates list for history Backfill : ${finalDatesList.mkString(COMMA)}")
      tgtDF.filter(col(dateCol).isin(finalDatesList: _*))
    } else {
      tgtDF
    }

    val newColsDf = getNewColumnsDf(finalTargetDf, transformedDf, primaryKeys)

    logger.info(s"Columns in newColsDf : ${newColsDf.columns.mkString(COMMA)}")

    if (Seq(DV_DEL_IND, MD5_HASH_ID).forall(transformedDf.columns.contains)) {
      finalTargetDf
        .join(newColsDf, primaryKeys, LEFT)
        .calculateMD5(primaryKeys)
        .withColumn(DV_DEL_IND, coalesce(col(DV_DEL_IND), lit(true)))
        .selectExpr(selectCols: _*)
    } else {
      finalTargetDf
        .join(newColsDf, primaryKeys, LEFT)
        .selectExpr(selectCols: _*)
    }

  }

  /**
   * Method to get previous snapshot version number of the Delta table before schema evolution
   *
   * @param tgtTbl  the target Delta table as String
   * @param runDate the run date of the schema evolution job
   * @return version number as Long
   */
  def getPrevWriteVersion(tgtTbl: String, runDate: LocalDate): Long = {

    val deltaHistDf        = DeltaTable.forName(tgtTbl).history()
    var finalVersion: Long = 0L

    val filterDf = deltaHistDf
      .filter(
        (col(OPERATION) === WRITE || col(OPERATION) === CONVERT || col(
          OPERATION
        ) === MERGE || col(OPERATION) === DROP_COLUMNS) && to_date(
          col(TIMESTAMP)
        ) >= runDate.toString
      )
      .select(VERSION, OPERATION_PARAMETERS, OPERATION)
      .withColumn(SPLIT_COL, col(OPERATION_PARAMETERS).getField(PARTITIONBY))

    if (!(filterDf.filter(col(OPERATION) === DROP_COLUMNS).isEmpty)) {
      val version =
        filterDf.filter(col(OPERATION) === DROP_COLUMNS).select(VERSION).collect()(0)(0).toString
      finalVersion = version.toLong - NBR_ONE
    } else if (filterDf.filter(col(SPLIT_COL) =!= SQUARE_BRACES).isEmpty) {
      val version =
        deltaHistDf
          .filter(
            col(OPERATION) === WRITE || col(OPERATION) === CONVERT || col(OPERATION) === MERGE
              || col(OPERATION) === RESTORE
          )
          .agg(max(VERSION))
          .collect()(0)(0)
          .toString
      finalVersion = version.toLong
    } else {
      val version =
        filterDf.filter(col(SPLIT_COL) =!= SQUARE_BRACES).select(VERSION).collect()(0)(0).toString
      finalVersion = version.toLong - NBR_ONE
    }

    finalVersion
  }

  /**
   * Method to evolve Schema using Overwrite Schema for Delta format tables
   *
   * @param evolvedDf     the new dataframe with the incoming schema changes
   * @param tgtTbl        the target table
   * @param partitionCols if table is partitioned then Sequence of Partition Cols to be passed else by default it is empty Seq
   * Note : Assumption is that the incoming evolvedDf is already optimised on partitions.
   */
  @deprecated
  def evolveTarget(
      evolvedDf: DataFrame,
      tgtTbl: String,
      partitionCols: Seq[String] = Seq()
  ): Unit = {

    val gcsTblLocation = getTableLocation(tgtTbl)

    if (partitionCols.nonEmpty) {
      evolvedDf.write
        .mode(OVERWRITE)
        .format(DELTA_FORMAT)
        .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, DYNAMIC)
        .partitionBy(partitionCols: _*)
        .option(OVERWRITE_SCHEMA, "true")
        .save(gcsTblLocation)
    } else {
      evolvedDf.write
        .mode(OVERWRITE)
        .format(DELTA_FORMAT)
        .option(OVERWRITE_SCHEMA, "true")
        .save(gcsTblLocation)
    }

  }

  /**
   * Checks if Delta table was already overwritten with new schema
   *
   * @param tgtTbl  the target Delta table as String
   * @param runDate the run date of the schema evolution job
   * @return Boolean value, true if schema is already overwritten for the given run date
   */

  def checkSchemaOverwrite(tgtTbl: String, runDate: LocalDate): Boolean = {

    val deltaHistDf = DeltaTable.forName(tgtTbl).history()

    val checkBackfillDf = deltaHistDf
      .filter(col(OPERATION) === WRITE && to_date(col(TIMESTAMP)) >= runDate.toString)
      .select(OPERATION_PARAMETERS)
      .withColumn(SPLIT_COL, col(OPERATION_PARAMETERS).getField(PARTITIONBY))
      .filter(col(SPLIT_COL) =!= SQUARE_BRACES)

    if (!checkBackfillDf.isEmpty) {
      true
    } else {
      false
    }

  }

  /**
   * Returns the last processed row timestamp for a dataframe as the UPD_TS for updated rows,
   * current timestamp if no rows were updated or None if dataframe is empty
   * This method is specific to Delta tables where delta flag is not there.
   *
   * @param finalDf Dataframe for which row ts needs to be returned
   * @param tgtTbl the target Delta table name as String
   * @return Row timestamp for given dataframe
   */
  def getLastPrcsdRowTsDelta(finalDf: DataFrame, tgtTbl: String): Timestamp = {

    val deltaHistDf = DeltaTable.forName(tgtTbl).history()

    val version =
      deltaHistDf.agg(max(VERSION)).collect()(0)(0).toString

    if (version.nonEmpty) {
      val compareTimestamp =
        deltaHistDf.filter(col(VERSION) === version).select(TIMESTAMP).collect()(0)(0)

      if (!finalDf.filter(col(UPD_TS) > compareTimestamp).isEmpty) {
        finalDf.filter(col(UPD_TS) > compareTimestamp).select(UPD_TS).head().getTimestamp(0)
      } else {
        Timestamp.valueOf(LocalDateTime.now())
      }
    } else {
      Timestamp.valueOf(LocalDateTime.now())
    }
  }

  /**
   * This method returns geo region code and op_cpmny_cd values derived from command line configs
   * @param config CommandLineConfigs
   * @return A tuple of strings with geo_region_cd and op_cmpny_cd values.
   */
  def getRegionOpCmpnyCd(config: CommandLineConfigs): (String, String) = {
    val geoRegionCd = config.region.getOrElse(US)
    logger.info(s"geoRegionCd is ${geoRegionCd}")
    val tenantConfig = config.tenant.getOrElse(WMT)
    val opCmpnyCd = if (tenantConfig.contains(DOT_COM)) {
      tenantConfig
    } else {
      tenantConfig.concat(HYPHEN).concat(geoRegionCd)
    }
    logger.info(s"opCmpnyCd is ${opCmpnyCd}")
    (geoRegionCd, opCmpnyCd)
  }

  /**
   * this method returns clusterName based on the infra
   * currently supports dataproc, databricks and dataproc-serverless
   * @return clusterName as String
   */
  def getClusterName: String = {
    val spark: SparkSession = SparkSession.getActiveSession.get
    val clusterName = if (spark.conf.getAll.contains("spark.hadoop.dfs.nameservices")) {
      spark.conf.get("spark.hadoop.dfs.nameservices")
    } else if (spark.conf.getAll.contains("spark.databricks.clusterUsageTags.clusterName")) {
      spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
    } else if (spark.conf.getAll.contains("spark.eventLog.dir")) {
      spark.conf.get("spark.eventLog.dir").split(FORWARD_SLASH).last
    } else {
      "NoClusterFound"
    }
    clusterName
  }

  /**
   * Implicit class DeltaFunctions provides set of functions to perform Change Data Capture (CDC) from traditional
   * Data warehousing practices viz. Slow Changing Dimension (SCD) Type 1, Type2 etc.
   * Provides chain functions on top DataFrame type viz.
   * [[com.vitthalmirji.luminate.cperf.spark.ETL.DeltaTransformations#performDelta(scala.Option, scala.collection.immutable.List, scala.Option)]]
   *
   * @param dataframe Dataset transformed / fetched from source fresh
   */
  implicit class DeltaTransformations(dataframe: DataFrame) extends Serializable {

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL#performScdType1Delta(org.apache.spark.sql.Dataset, org.apache.spark.sql.Dataset,
     * scala.collection.immutable.List, scala.Option)]]
     * Function that performs Slow Changing Dimension Type 1 Delta on given datasets
     * TODO SCD type 2 Delta
     *
     * @param previousLoadedDataset             Dataset that was already loaded previously for comparision at row/value level
     *                                          This value will be None during initial load / history loads
     * @param primaryKeys                       List of Primary keys common between both datasets
     * @param columnsExcludeHashcodeCalculation List of Columns that must be skipped during hashcode computation
     * @return Dataset with column `delta_flag` having values `I`, `U`, `D`, `NC`
     *         Usage:
     *         For Initial / History load:
     *         val sourceQuery = "SELECT t1.*, current_timestamp AS load_ts, current_timestamp AS upd_ts FROM source.test_source t1"
     *         val extractDf = spark.sql(sourceQuery)
     *         val deltaDf = extractDf.performDeltaType1SCD(previousLoadedDataset = None, primaryKeys = List("id", "bus_dt"),
     *         columnsExcludeHashcodeCalculation = None)
     *         deltaDf.show(false)
     *         deltaDf
     *         .select(spark.dataframe("ww_chnl_perf_app.test").columns.map(col):_*)
     *         .write
     *         .mode("overwrite")
     *         .format("orc")
     *         .insertInto("ww_chnl_perf_app.test")
     *
     *         val validateDf = spark.sql("SELECT DISTINCT delta_flag FROM ww_chnl_perf_app.test")
     *         validateDf.show(false)
     *         assert(validateDf.count().equals(1L))
     *         assert(validateDf.take(1)(0).getAs[String]("delta_flag").equals("I"))
     *         For Change Data capture subsequent loads:
     *         val sourceQuery =
     *         """SELECT t1.id, t1.name, t1.op_cmpny_cd, t1.bus_dt,
     *         |CASE WHEN t1.name = 'Christian' THEN 31 ELSE t1.age END AS age,
     *         |current_timestamp AS load_ts, current_timestamp AS upd_ts FROM source.test_source t1
     *         |WHERE t1.name <> 'Bob'
     *         |UNION ALL
     *         |SELECT 10 AS id, 'Ryan' AS name, 'WMT-US' AS op_cmpny_cd, '2021-01-11' AS bus_dt,
     *         |29 AS age, current_timestamp AS load_ts, current_timestamp AS upd_ts
     *         |""".stripMargin
     *         val extractDf = spark.sql(sourceQuery)
     *
     *         val targetTable = spark.dataframe("ww_chnl_perf_app.test")
     *
     *         val partitionsToDrop = targetTable
     *         .selectExpr("CAST (bus_dt AS STRING) AS bus_dt")
     *         .distinct()
     *         .take(100)
     *         .map(_.getAs[String]("bus_dt"))
     *         .map(dt => Partition(Map("bus_dt" -> dt))).toList
     *
     *         partitionsToDrop.drop("ww_chnl_perf_stg.test_bkp")
     *         partitionsToDrop.delete("ww_chnl_perf_stg.test_bkp")
     *
     *         targetTable.write.mode("overwrite").format("orc").insertInto("ww_chnl_perf_stg.test_bkp")
     *         val previousLoadedDf = spark.dataframe("ww_chnl_perf_stg.test_bkp")
     *         assert(previousLoadedDf.count() > 0)
     *
     *         val deltaDf = extractDf.performDeltaType1SCD(previousLoadedDataset = Some(previousLoadedDf),
     *         primaryKeys = List("id", "bus_dt"), columnsExcludeHashcodeCalculation = None)
     *         deltaDf.show(false)
     *
     *         partitionsToDrop.drop("ww_chnl_perf_app.test")
     *         partitionsToDrop.delete("ww_chnl_perf_app.test")
     *
     *         deltaDf
     *         .select(spark.dataframe("ww_chnl_perf_app.test").columns.map(col):_*)
     *         .write
     *         .mode("overwrite")
     *         .format("orc")
     *         .insertInto("ww_chnl_perf_app.test")
     *
     *         val validateDf = spark.sql("SELECT * FROM ww_chnl_perf_app.test")
     *         validateDf.show(false)
     *         assert(validateDf.count() > 0)
     *         assert(validateDf
     *         .select("delta_flag")
     *         .distinct()
     *         .count()
     *         .equals(4L))
     *         assert(
     *         validateDf
     *         .select("delta_flag")
     *         .distinct()
     *         .take(4)
     *         .map(_.getAs[String]("delta_flag"))
     *         .sorted
     *         .sameElements(Array("D", "I", "NC", "U"))
     *         )
     */
    def performDelta(
        previousLoadedDataset: Option[DataFrame],
        primaryKeys: List[String],
        columnsExcludeHashcodeCalculation: Option[List[String]] = None
    ): DataFrame = {
      val deltaDf = previousLoadedDataset.map { previousDataset =>
        if (primaryKeys.isEmpty) {
          val errorMessage = "Primary keys cannot be empty"
          logger.error(errorMessage)
          throw new IllegalArgumentException(errorMessage)
        }

        val newDatasetColumns = dataframe.columns
        logger.warn(s"Source columns = ${newDatasetColumns.mkString(COMMA)}")
        val previousLoadedDatasetColumns = previousDataset.columns.filterNot(_.equals(DELTA))
        logger.warn(
          s"Stage columns except delta_flag = ${previousLoadedDatasetColumns.mkString(COMMA)}"
        )
        val hashColumnsList = newDatasetColumns
          .filterNot(c =>
            (primaryKeys ++ AUDIT_COLUMN_NAMES ++ Array(
              DV_DEL_IND,
              MD5_HASH_ID
            ) ++ columnsExcludeHashcodeCalculation
              .getOrElse(List())).contains(c)
          )
        var joinExpr: Column = lit(true)
        primaryKeys.foreach(k => joinExpr = (col(s"$SRC.$k") === col(s"$STG.$k")) && joinExpr)

        logger.info(s"Hashcode will be computed on columns ${hashColumnsList.mkString(COMMA)}")

        if (
          primaryKeys
            .map(k => newDatasetColumns.contains(k) && previousLoadedDatasetColumns.contains(k))
            .exists(_.equals(false))
        ) {
          val errorMessage = s"Primary keys given are missing in datasets $primaryKeys"
          logger.error(errorMessage)
          throw new IllegalArgumentException(errorMessage)
        }

        val sourceDf = dataframe
          .withColumn(
            HASHCODE,
            hash64(concat_ws(COLUMNS_SEPARATOR_CHARACTER, hashColumnsList.map(col): _*))
          )
        val stageDf = if (previousDataset.columns.contains(DELTA)) {
          previousDataset
            .filter(s"`$DELTA` <> 'D'")
            .drop(DELTA)
            .withColumn(
              HASHCODE,
              hash64(concat_ws(COLUMNS_SEPARATOR_CHARACTER, hashColumnsList.map(col): _*))
            )
        } else {
          previousDataset
            .withColumn(
              HASHCODE,
              hash64(concat_ws(COLUMNS_SEPARATOR_CHARACTER, hashColumnsList.map(col): _*))
            )
        }

        val deltaDf = sourceDf
          .as(SRC)
          .join(stageDf.as(STG), joinExpr, FULL_OUTER)
          .withColumn(
            DELTA,
            when(col(s"$STG.${primaryKeys.head}").isNull, INSERT_FLAG)
              .when(col(s"$SRC.${primaryKeys.head}").isNull, DELETE_FLAG)
              .when(col(s"$SRC.$HASHCODE") === col(s"$STG.$HASHCODE"), NO_CHANGE_FLAG)
              .otherwise(UPDATE_FLAG)
          )
          .select((previousLoadedDatasetColumns ++ Array(DELTA)).map(resolveAuditColumns): _*)

        if (logger.isDebugEnabled) {
          deltaDf.printSchema()
        }
        deltaDf.selectExpr(newDatasetColumns: _*)
      }
      deltaDf.getOrElse(dataframe.withColumn(DELTA, lit(INSERT_FLAG)))
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL#hash64(scala.collection.Seq)]] function that computes Hash64 using spark's
     * [[org.apache.spark.sql.catalyst.expressions.XxHash64#XxHash64(scala.collection.Seq)]]
     * Private function, not available for end users
     * TODO Change hashing logic: Try FNV / Murmur
     *
     * @param cols list of columns on which Hash64 myst be computed
     * @return a Spark SQL Column containing Hash64 value
     *         Usage:
     *         val sourceDf = newDataset.withColumn(HASHCODE, hash64(concat_ws("~", hashColumnsList.map(col): _*)))
     */
    private def hash64(cols: Column*): Column = new Column(new XxHash64(cols.map(_.expr)))

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL#resolveAuditColumns(java.lang.String)]] function resolves selecting column with aliases
     * after any SQL JOIN
     * Private function, should not be used by End users
     *
     * @param columnName name of the field / column to resolve
     * @return a Spark SQL Column containing resolved value from particular dataset
     *         Usage:
     *         val deltaDf = sourceDf.as(SRC).join(stageDf.as(STG), joinExpr, FULL_OUTER)
     *         .withColumn(DELTA_FLAG, when(col(s"$STG.${primaryKeys.head}").isNull, INSERT_FLAG)
     *         .when(col(s"$SRC.${primaryKeys.head}").isNull, DELETE_FLAG)
     *         .when(col(s"$SRC.$HASHCODE") === col(s"$STG.$HASHCODE"), NO_CHANGE_FLAG).otherwise(UPDATE_FLAG))
     *         .select(previousLoadedDatasetColumns.map(resolveAuditColumns) :_*)
     */
    private def resolveAuditColumns(columnName: String): Column = {
      logger.warn(columnName)
      val resolvedColumn = columnName match {
        case DELTA => col(DELTA)
        case UPD_TS =>
          when(
            col(DELTA).isin(INSERT_FLAG, UPDATE_FLAG, DELETE_FLAG),
            to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT))
          )
            .otherwise(col(s"$STG.$columnName"))
            .alias(columnName)
        case LOAD_TS =>
          when(
            col(DELTA).isin(INSERT_FLAG),
            to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT))
          )
            .otherwise(col(s"$STG.$columnName"))
            .alias(columnName)
        case DV_DEL_IND =>
          when(col(DELTA).isin(DELETE_FLAG), lit(TRUE))
            .otherwise(lit(FALSE))
            .alias(columnName)
        case _ =>
          when(col(DELTA).isin(INSERT_FLAG, UPDATE_FLAG, NO_CHANGE_FLAG), col(s"$SRC.$columnName"))
            .otherwise(col(s"$STG.$columnName"))
            .alias(columnName)
      }
      resolvedColumn
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL#performDeltaUsingCheckSum(org.apache.spark.sql.Dataset,scala.collection.Seq)]]
     * function that computes the delta changes using md5 checksum column

     * @param targetDf existing target table data as a dataframe
     * @param primaryCols Seq of primary key columns for the given table
     * @return a Spark dataframe with the updated audit columns as per the logic
     *         Usage:
     *         val newDf = new data got from source
     *         newDf.performDeltaUsingCheckSum(targetDf, Seq("id"))
     */
    def performDeltaUsingCheckSum(targetDf: DataFrame, primaryCols: Seq[String]): DataFrame = {
      val deltaSelectCols = primaryCols :+ MD5_HASH_ID :+ LOAD_TS :+ UPD_TS :+ USER_ID
      dataframe
        .drop(Seq(LOAD_TS, UPD_TS, DV_DEL_IND, USER_ID): _*)
        .join(
          targetDf
            .filter(s"`$DV_DEL_IND` <> 'true'")
            .selectExpr(deltaSelectCols: _*),
          primaryCols,
          joinType = FULL_OUTER
        )
        .withColumn(
          DV_DEL_IND,
          when(dataframe(MD5_HASH_ID).isNull, lit(TRUE)).otherwise(lit(FALSE))
        )
        .withColumn(
          LOAD_TS,
          when(
            targetDf(MD5_HASH_ID).isNull,
            to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT))
          )
            .otherwise(targetDf(LOAD_TS))
        )
        .withColumn(
          UPD_TS,
          when(dataframe(MD5_HASH_ID) === targetDf(MD5_HASH_ID), targetDf(UPD_TS))
            .otherwise(
              to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT))
            )
        )
        .withColumn(
          USER_ID,
          when(dataframe(MD5_HASH_ID) === targetDf(MD5_HASH_ID), targetDf(USER_ID))
            .otherwise(
              lit(SparkSession.getActiveSession.get.sparkContext.sparkUser)
            )
        )
        .drop(targetDf(MD5_HASH_ID))
    }

    /**
     * Derives and adds delta flag column to dataframe.
     * Delta flag can have 4 possible values - I (Insert), U (Update), D (Delete), NC (No Change).
     * In case of history refresh type, delta flag for all records will be I (Insert).
     * In case of release refresh type, delta flag for all records will be U (Update).
     * In case of any other refresh type, no modifications will be made to the dataframe.
     * @param refreshType Current refresh type for run
     * @return Dataframe with delta flag column, if applicable, based on refresh type.
     */
    def computeDeltaFlagForRefreshType(refreshType: String): DataFrame =
      if (refreshType.equalsIgnoreCase(HISTORY)) {
        dataframe.withColumn(DELTA, lit(INSERT_FLAG))
      } else if (refreshType.equalsIgnoreCase(RELEASE)) {
        dataframe.withColumn(DELTA, lit(UPDATE_FLAG))
      } else {
        dataframe
      }

    /**
     * Derives and adds delta flag column to dataframe.
     * Delta flag can have 4 possible values - I (Insert), U (Update), D (Delete), NC (No Change).
     * Delta flag will be derived as follows -
     *  NC => when upd_ts of a record is earlier than or equal to `lastPrcsdRowTs`
     *  D => when column [[com.vitthalmirji.luminate.cperf.constants.StringConstants#DV_DEL_IND()]] is true
     *  I => when load_ts and upd_ts for a record are same
     *  U => when load_ts and upd_ts for a record are different
     * In case `lastPrcsdRowTs` is not provided, the assumption will be that only changed records are
     * being passed to this utility, and hence only 3 flags (I, U, D) will be derived.
     * In case [[DV_DEL_IND]] column is not present in the dataframe, the D flag will not be calculated.
     * @param lastPrcsdRowTs The last processed row ts from last run, used to determine NC records.
     * @return Dataframe with delta flag column.
     */
    def computeDeltaFlag(
        lastPrcsdRowTs: Option[Timestamp] = None
    ): DataFrame = {

      val deleteAndNoChangeCase =
        if (lastPrcsdRowTs.isDefined && dataframe.columns.contains(DV_DEL_IND)) {
          when(
            col(UPD_TS) <= lastPrcsdRowTs.get,
            lit(NO_CHANGE_FLAG)
          ).when(
            col(DV_DEL_IND) === TRUE,
            lit(DELETE_FLAG)
          )
        } else if (lastPrcsdRowTs.isDefined) {
          when(
            col(UPD_TS) <= lastPrcsdRowTs.get,
            lit(NO_CHANGE_FLAG)
          )
        } else if (dataframe.columns.contains(DV_DEL_IND)) {
          when(
            col(DV_DEL_IND) === TRUE,
            lit(DELETE_FLAG)
          )
        } else {
          when(
            lit(FALSE),
            lit(INSERT_FLAG)
          )
        }

      val deltaFlagColumn = deleteAndNoChangeCase
        .when(
          col(LOAD_TS) === col(UPD_TS),
          lit(INSERT_FLAG)
        )
        .otherwise(lit(UPDATE_FLAG))

      dataframe
        .withColumn(DELTA, deltaFlagColumn)
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL.DeltaTransformations#mutateAuditColumnsWithMD5(Seq[String])]] adds audit columns with new values
     * and md5 checksum
     * @param columnsToExclude Set of columns to be excluded while computing MD5 checksum
     * @param flagRequired Boolean which indicates whether delta flag is required or not. Default is false
     * @return Returns dataframe added with Audit columns having new values
     *         Usage:
     *         val df = spark.sql("< some query >")
     *         val dfWithAuditColumns = df.mutateAuditColumnsWithMD5(Seq("id"))
     *         In case delta_flag is required,
     *         val dfWithAuditColumns = df.mutateAuditColumnsWithMD5(Seq("id"),true)

     */
    def mutateAuditColumnsWithMD5(
        columnsToExclude: Seq[String],
        flagRequired: Boolean = false
    ): DataFrame = {
      val auditDf = dataframe
        .calculateMD5(columnsToExclude)
        .withColumn(DV_DEL_IND, lit(FALSE))
        .mutateAuditColumns
      if (flagRequired) {
        auditDf
      } else {
        auditDf
          .drop(DELTA)
      }
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL.DeltaTransformations#mutateAuditColumns()]] adds audit columns with new values
     *
     * @return Returns dataframe added with Audit columns having new values
     *         Usage:
     *         val df = spark.sql("< some query >")
     *         val dfWithAuditColumns = df.mutateAuditColumns
     */
    def mutateAuditColumns: DataFrame =
      dataframe
        .withColumn(USER_ID, lit(SparkSession.getActiveSession.get.sparkContext.sparkUser))
        .withColumn(
          LOAD_TS,
          to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT))
        )
        .withColumn(
          UPD_TS,
          to_timestamp(date_format(current_timestamp, YEAR_MONTH_DATE_24HOUR_TS_FORMAT))
        )
        .withColumn(DELTA, lit(INSERT_FLAG))

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL.DeltaTransformations#calculateMD5(Seq[String])]] adds md5 checksum value for the given set of columns
     * except the passed primary key set and in-built audit columns
     * @param columnsToExclude Set of columns to be excluded while computing MD5 checksum
     * @return Returns dataframe added with the new computed MD5 value
     *         Usage:
     *         val df = spark.sql("< some query >")
     *         val dfWithMD5 = df.calculateMD5
     */
    def calculateMD5(columnsToExclude: Seq[String]): DataFrame =
      dataframe.withColumn(
        MD5_HASH_ID,
        md5(
          concat_ws(
            "_",
            dataframe.columns
              .filterNot((columnsToExclude ++ NEW_AUDIT_COLUMN_NAMES).contains(_))
              .sorted
              .map(a =>
                when(col(a).isNull, lit(MD5_HASH_SEPERATOR))
                  .otherwise(concat(lit(MD5_HASH_SEPERATOR), col(a).cast(StringType)))
              ): _*
          )
        )
      )
  }

  /**
   * [[com.vitthalmirji.luminate.cperf.spark.ETL.ETLDataframeActions]] Implicit class provides set of functions on top of Dataframe
   * @param dataframe Dataframe queried from table / produced
   */
  implicit class ETLDataframeActions(dataframe: DataFrame) extends Serializable {

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL.ETLDataframeActions#backup(org.apache.spark.sql.Dataset, java.lang.String)]]
     *
     * @param backupLocationRootPath Location to copy/backup data
     * @return Returns boolean if all underlying files of external dataframe copied successful, also prints file-locations which did not succeed copy
     *         Usage:
     *         val df = spark.sql("SELECT *FROM ww_chnl_perf_app.test")
     *         df.backup("gs://bucket/ww_chnl_perf_app_qa.db/test_backup_user_given_date/")
     *         Note: DO NOT FORGET TO ADD FORWARD SLASH IF TARGET PATH IS DIRECTORY example dir2/
     */
    def backup(backupLocationRootPath: String): Boolean = {
      val spark: SparkSession  = SparkSession.getActiveSession.get
      val SOURCE_FILE_LOCATION = "source_file_location"
      val TARGET_FILE_LOCATION = "target_file_location"

      val targetBaseLocation = if (backupLocationRootPath.endsWith(FORWARD_SLASH)) {
        backupLocationRootPath.stripSuffix(FORWARD_SLASH)
      } else { backupLocationRootPath }

      val dataframeBaseLocation = getLocation
      if (dataframeBaseLocation.isEmpty) {
        throw new Exception("Unable to identify base location of dataframe")
      }

      logger.warn(s"Dataframe base location: $dataframeBaseLocation")
      logger.warn(s"Backup location: $targetBaseLocation")

      import spark.implicits.newProductEncoder
      val fileLocations = dataframe
        .select(input_file_name().alias(SOURCE_FILE_LOCATION))
        .distinct()
        .selectExpr(
          SOURCE_FILE_LOCATION,
          s"replace($SOURCE_FILE_LOCATION, '${dataframeBaseLocation.get}', '$targetBaseLocation') AS $TARGET_FILE_LOCATION"
        )
        .as[(String, String)]
        .rdd
      parallelCopy(fileLocations)
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL.ETLDataframeActions#getLocation()]]
     * Identifies the Location of the files read into dataframe.
     * Can be used for both Dataframe created Reading from table and dataframe created reading from files.
     * @return Location of the files read into dataframe
     */
    def getLocation: Option[String] = {
      val dataframeTableLocation =
        try
          dataframe
            .getDataframeMetadataProperty[TableIdentifier]
            .map(table => getTableLocation(table.toString()))
        catch {
          case _: Throwable =>
            logger.error(
              "Error fetching location from dataframe. The dataframe may be created reading from files, attempting once again"
            )
            None
          case _: Throwable => None
        }

      if (dataframeTableLocation.isEmpty) {
        try
          dataframe
            .getDataframeMetadataProperty[Map[String, String]]
            .flatMap(_.get("Location").flatMap { location =>
              val regexPattern = "(?<=\\[).+?(?=\\])".r
              regexPattern.findFirstIn(location).map(_.trim)
            })
        catch {
          case _: Throwable =>
            logger.error("Error fetching location from dataframe.")
            None
          case _: Throwable => None
        }
      } else {
        dataframeTableLocation
      }
    }

    /**
     * [[com.vitthalmirji.luminate.cperf.spark.ETL.ETLDataframeActions#getDataframeMetadataProperty(scala.reflect.api.TypeTags.TypeTag)]]
     * @return Returns Option of table / dataframe properties
     *         Usage:
     *         val df = spark.sql("SELECT *FROM schema.test")
     *         df.getTableMetadataProperty[URI] - Should return location of table gs://< absolute-path >
     *         df.getTableMetadataProperty[TableIdentifier] - Should return name of table `schema`.`test`
     *         df.getTableMetadataProperty[CatalogTable] - Should return CatalogTable type having properties viz.
     *         BucketSpec, Table Statistics, Create Time, Storage Type, Table Type, Partition Column Names etc.
     *         In case of dataframe created by reading files:
     *         val df = spark.read.parquet(< path >)
     *         df.getTableMetadataProperty[ Map[ String,String ] ].get("Location")
     *         Should return location of base directory of files
     */
    def getDataframeMetadataProperty[T](implicit property: TypeTag[T]): Option[T] = {

      /**
       * [[getProperty]]
       * @param property Type of Property from Dataframe Catalog's metadata
       * @param catalogTable Table catalog
       * @return Returns property of dataframe/table's catalog
       */
      def getProperty(property: TypeTag[T], catalogTable: CatalogTable): T =
        property.tpe match {
          case t: Any if t =:= typeOf[CatalogTable]    => catalogTable.asInstanceOf[T]
          case t: Any if t =:= typeOf[URI]             => catalogTable.location.asInstanceOf[T]
          case t: Any if t =:= typeOf[TableIdentifier] => catalogTable.identifier.asInstanceOf[T]
        }

      Seq(
        dataframe.queryExecution.optimizedPlan,
        dataframe.queryExecution.logical,
        dataframe.queryExecution.sparkPlan
      ).flatMap { plan =>
        plan.collect {
          case LogicalRelation(_, _, catalogTable: Option[CatalogTable], _) =>
            catalogTable.map(getProperty(property, _))
          case hiveTableRelation: HiveTableRelation =>
            Some(getProperty(property, hiveTableRelation.tableMeta))
          case fileSourceScanExec: FileSourceScanExec =>
            Some(fileSourceScanExec.metadata.asInstanceOf[T])
        }
      }.filter(_.isDefined).head
    }
  }

  /**
   * Method to determine the table type.
   * If the table is of type ICEBERG, DELTA, PARQUET, ORC, AVRO, CSV, or JSON, it returns the type.
   * Otherwise, it throws an exception.
   * @param tableName - The table name with schema.
   * @return The table type.
   * @throws Exception if the table type is unsupported.
   */

  def getTableType(tableName: String): String =
    try {
      val provider = getTableDetail(tableName, PROVIDER).toUpperCase
      val msg =
        " Please provide a table from the supported formats: ORC, AVRO, CSV, PARQUET, JSON, DELTA, ICEBERG."

      provider match {
        case BQ.DELTA_FORMAT => BQ.DELTA_FORMAT
        case ICEBERG         => ICEBERG
        case HIVE =>
          getHiveFileFormat(tableName, msg)
        case _ =>
          throwExpection(s"Unknown format with provider:$PROVIDER." + msg)
      }
    } catch {
      case e: Exception =>
        val msg = s"getTableType failed with: ${e.getMessage}"
        logger.info(msg)
        throw new Exception(msg)
    }

  private def getHiveFileFormat(tableName: String, msg: String) = {
    val serde       = getTableDetail(tableName, SERDE_LIBRARY).toUpperCase
    val inputFormat = getTableDetail(tableName, INPUT_FORMAT)

    inputFormat match {
      case ORC_INPUT_FORMAT     => ORC
      case PARQUET_INPUT_FORMAT => BQ.PARQUET
      case AVRO_INPUT_FORMAT    => AVRO
      case _ =>
        if (
          serde.contains(OPENCSV_SERDE) || serde.contains(LAZY_SIMPLE_SERDE) ||
          serde.contains(REGEX_SERDE)
        ) {
          CSV
        } else if (
          serde.contains(JSON) || serde.contains(OPENX_JSON) ||
          serde.contains(GSON) || serde.contains(JACKSON) ||
          serde.contains(JSON_SERDE)
        ) {
          JSON
        } else {
          throwExpection(
            s"Unknown format with SerDe: $serde, InputFormat: $inputFormat." + msg
          )

        }
    }
  }

}

class ETL {
  def executeBackup(df: DataFrame, backupLocation: String): Boolean =
    df.backup(backupLocation)
}

case class CommandLineConfigs(
    workflow: String = "",
    refreshType: String = "",
    runMode: String = "",
    dqEnabled: Boolean = true,
    startDate: LocalDate = NULL_VAL,
    endDate: LocalDate = NULL_VAL,
    tenant: Option[String] = None,
    region: Option[String] = None,
    runDate: LocalDate = LocalDate.now(),
    globalDuns: Option[Array[String]] = None,
    feedName: Option[String] = None,
    supplierNames: Option[Array[String]] = None,
    fileFormats: Option[Array[String]] = None,
    overwriteData: Option[Boolean] = None
)

/**
 * Column names config for backup utility
 * @param dfDatesCol Dates column name in dates df
 * @param dfRefreshTypeCol Refresh type column name in dates df
 * @param tableDatesCol Dates column name in backup dates table
 * @param tableRefreshTypeCol Refresh type column name in backup dates table
 * @param rowTsCol Row Ts column name in backup dates table
 * @param partTsCol Part Ts column name in backup dates table
 * @param datePartCol Date partition column name in target table
 */
case class BackupConfig(
    dfDatesCol: String = DATES,
    dfRefreshTypeCol: String = REFRESH_TYPE,
    tableDatesCol: String = DATES,
    tableRefreshTypeCol: String = REFRESH_TYPE,
    rowTsCol: String = ROW_TS,
    partTsCol: String = PART_TS,
    datePartCol: String = BUS_DT
)
