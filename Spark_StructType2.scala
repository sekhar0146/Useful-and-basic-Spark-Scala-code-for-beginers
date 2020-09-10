package practise

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Row, SparkSession}

object Spark_StructType2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    // RDD: employee data
    val empRDD = spark.sparkContext.textFile("C:/Users/z011348/Desktop/Spark/temp.txt")

    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

    // Generate schema
    val schema =
      StructType(
        Seq(
        StructField("eno", IntegerType, nullable = false),
        StructField("ename", StringType, nullable = false),
        StructField("eloc", StringType, nullable = true),
        StructField("esal", IntegerType, nullable = false)
        )
      )

    // Convert records of the RDD to Rows
    val rowRDD = empRDD
      .map(p => p.split(","))
      .map(p => Row(p(0).toInt, p(1), p(2), p(3).toInt))
    rowRDD.collect().foreach(println)

    val empDF = spark.createDataFrame(rowRDD, schema)
    empDF.show()
    empDF.printSchema()
  }

}