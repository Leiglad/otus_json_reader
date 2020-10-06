package com.github.mrpowers.my.cool.project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization



case class User(id: Option[Integer],
                country : Option[String],
                points: Option[Integer],
                title: Option[String],
                variety: Option[String],
                winery: Option[String]
               )
object JsonReader extends App with SparkSessionWrapper {
  if (args.length == 0) {
    println("need path to data file")
    sys.exit(1)
  }
  val filename = args(0)

  implicit val formats: AnyRef with Formats = {
    Serialization.formats(FullTypeHints(List(classOf[User])))
  }
  //import spark.implicits._
  val json: RDD[String] = spark.sparkContext.textFile(filename)

  json.foreach(str => {
    val decodedUser = parse(str).extract[User]
    println(decodedUser)
  }
  )

}

