import Model.{EventAndChannel, FieldInfo}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import spray.json._
import DefaultJsonProtocol._

import scala.util.Try

object JsonReader {

  val spark: SparkSession = SparkSession.builder
    .appName("oofff")
    .master("local[2]")
    .getOrCreate()

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat3(FieldInfo)
  }

  object EventAndChannelProtocol extends DefaultJsonProtocol {
    implicit val colorFormat2 = jsonFormat2(EventAndChannel)
  }


  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val dataset = spark.sparkContext.textFile("part-**")
    import MyJsonProtocol._


    val parseJsonDS = dataset.map {
      row =>
        val fields = row.parseJson.asJsObject.getFields("fields").toList.head.toString().parseJson.convertTo[List[FieldInfo]]
        val key = row.parseJson.asJsObject.getFields("key").toList.head.toString()
        val count = row.parseJson.asJsObject.getFields("count").head.toString().toInt
        Try((key, count, fields))
    }.filter(_.isSuccess).map(_.get).toDS().as[(String, Int, List[FieldInfo])]

    val groupedDS = parseJsonDS.groupByKey {
      case (key: String, count: Int, fields: List[FieldInfo]) =>
        key
    }.mapValues {
      case (key: String, count: Int, fields: List[FieldInfo]) =>
        (count, fields)
    }.mapGroups {
      case (key, values) =>
        (key, values.toList)
    }

    groupedDS.map {
      case (key: String, vals: List[(Int, List[FieldInfo])]) =>
        val count = vals.map {
          w => w._1
        }.sum
        val flatt = vals.flatMap {
          w => w._2
        }.groupBy(fieldInfo => fieldInfo.key).map {
          list =>
            val isNotWritten = list._2.map {
              fieldInfo => fieldInfo.isNotWritten
            }.sum
            val isWritten = list._2.map {
              fieldInfo => fieldInfo.isWritten
            }.sum
            FieldInfo(list._1, isWritten, isNotWritten)
        }.toList
        val k = key.replace("\"", "").replace("[", "").replace("]", "")
        (k, count, flatt)
    }.toDF("eventAndChannel", "count", "fields").coalesce(1).write.json("finalResults.json")

  }
}
