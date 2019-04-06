package Helper

import java.util.Calendar

import Model.{EventAndChannel, Field, FieldInfo}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.hadoop.fs.s3a
import scala.util.Try

object Prepare {

  private val eventList = List(
    ""
  )

  /**
    * If necessary setup S3 connection
    *
    * @param sparkContext SparkContext
    * @param accesskey    String
    * @param secretKey    String
    */
  def setupHadoopEnvironment(sparkContext: SparkContext, accesskey: String, secretKey: String): Unit = {

    System.setProperty("com.amazonaws.services.s3.enableV4", "false")
    val hadoopConf = sparkContext.hadoopConfiguration

    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.awsAccessKeyId", accesskey)
    hadoopConf.set("fs.s3a.awsSecretAccessKey", secretKey)
    hadoopConf.set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
    hadoopConf.set("fs.s3a.region","eu-central-1")
  }

  /**
    * Read Files And Partition By pageType and channel
    *
    * @param spark SparkSession
    * @param path  String
    * @return Dataset[(PageTypeAndChannel, String)]
    */
  def readAndPartitionData(spark: SparkSession, path: String): Dataset[(EventAndChannel, Map[String, String])] = {
    import spark.implicits._

    val fileRDD = spark.sparkContext.textFile(path)
    val filteredDS = fileRDD.map {
      row =>
        val splittedRow = row.split("[|]{2,2}")
        val groupedRow = splittedRow.grouped(2)
        var states = scala.collection.mutable.Map[String, String]()
        val stateMap = groupedRow.map(m => m(0) -> Try(m(1)).getOrElse("")).toMap
        val event = Try(stateMap("event")).getOrElse("")
        val channel = Try(stateMap("channel")).getOrElse("")
        Try((EventAndChannel(event, channel), stateMap))
    }.filter(_.isSuccess).map(_.get).toDS().filter(_._1.event.nonEmpty)
    filteredDS
  }

  /**
    * Count of keys
    *
    * @param groupedDS KeyValueGroupedDataset[PageTypeAndChannel, (PageTypeAndChannel, String)]
    * @return Dataset[(PageTypeAndChannel, Long)]
    */
  def countOfGroups(groupedDS: KeyValueGroupedDataset[EventAndChannel, (EventAndChannel, Map[String, String])]):
  Dataset[(EventAndChannel, Long)] = {
    groupedDS.count()
  }

  /**
    * join count of pagetype and channel with fields
    *
    * @param countDS Dataset[(PageTypeAndChannel, Long)]
    * @param fieldDS DataFrame
    * @return DataFrame
    */
  def joinByPageTypeAndChannel(countDS: Dataset[(EventAndChannel, Long)],
                               fieldDS: DataFrame): DataFrame = {
     countDS.join(fieldDS, Seq("key"), "left")
  }

  /**
    * Map values to Field
    *
    * @param spark     SparkSession
    * @param groupedDS KeyValueGroupedDataset[EventAndChannel, (EventAndChannel, Map[String,String])]
    * @return
    */
  def mapValuesToField(spark: SparkSession, groupedDS: KeyValueGroupedDataset[EventAndChannel, (EventAndChannel, Map[String, String])]):
  KeyValueGroupedDataset[EventAndChannel, Array[Field]] = {
    import spark.implicits._
    groupedDS.mapValues {
      case (_, fieldMap) =>
        val fieldList = fieldMap.keys.filter(_.nonEmpty).map {
          key =>
            if (fieldMap(key).nonEmpty) Field(key, 1)
            else Field(key, 0)
        }
        fieldList.toArray
    }
  }

  /**
    * Create Final Dataset
    * @param spark SparkSession
    * @param keyAndFieldMap  KeyValueGroupedDataset[EventAndChannel,Array[Field]
    * @return
    */
  def createFinalDS(spark: SparkSession, keyAndFieldMap: KeyValueGroupedDataset[EventAndChannel, Array[Field]]): DataFrame = {
    import spark.implicits._
    keyAndFieldMap.mapGroups {
      case (key, values) =>
        val list = values.toList.flatMap {
          field =>
            field
        }.groupBy(identity)
          .mapValues(_.size)
          .toSeq
          .map {
            case (field: Field, count: Int) => Field(field.fieldName, count)
          }
        (key, list)
    }.toDF("key", "fields")

  }

  /**
    *
    * @param df   DataFrame
    * @param path String
    */
  def writeToPath(df: Dataset[(String,Long,List[FieldInfo])], path: String): Unit = {
    val p = s"$path"
//    df.write.option("delimiter","|").format("csv").save(p)
    df.coalesce(1).write.json(path)
  }
}


