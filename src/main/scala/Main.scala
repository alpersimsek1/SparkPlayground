import Helper.Prepare._
import Model.{EventAndChannel, Field, FieldInfo}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import Config._

import scala.util.Try
object Main {

  val spark: SparkSession = SparkSession.builder
    .appName("oofff")
    .getOrCreate()

  val listOfFilesStarters = List("0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f")

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val date = args(0)
    try {
      listOfFilesStarters.foreach {
        l =>
          val path = s"s3a://$l**"
          val rowDS = readAndPartitionData(spark, path)

          val groupedDS = rowDS.groupByKey {
            case (eventAndChannel: EventAndChannel, _) => eventAndChannel
          }

          val countOfGroupsDS = countOfGroups(groupedDS)

          val keyAndFieldMapDS = mapValuesToField(spark, groupedDS)

          val finalDS = createFinalDS(spark, keyAndFieldMapDS)

          val joinedDS = joinByPageTypeAndChannel(countOfGroupsDS, finalDS)
            .as[(String, Long, Array[Field])]

          val fDS = joinedDS.map {
            case (key, count, fields) =>
              val fieldArr = fields.map {
                f =>
                  FieldInfo(f.fieldName, f.isInTheList, count.toInt - f.isInTheList)
              }.toList
              (key, count, fieldArr)
          }.toDF("key", "count", "fields").as[(String,Long,List[FieldInfo])]
          writeToPath(fDS, s"s3a://$date/$l")
      }
    }
    catch {
      case e: Exception =>
        print(e)
    }
  }

}