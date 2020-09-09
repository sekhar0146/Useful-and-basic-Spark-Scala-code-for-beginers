package practise

import org.apache.spark.sql.SparkSession

object Spark_udf_call2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.sqlContext.implicits._

    // List to DataFrame
    val  empldf  =  List (
      (101,"KuMar",20000,"m",11),
      (102,"GanESh",80000,"m",12),
      (103,"KalYAni",10000,"f",13),
      (105,"RaJ",66000,"m",14)).toDF("eno", "ename", "esal", "egen", "edpt")

    /* register UDF */
    import org.apache.spark.sql.functions.udf
    val ENameCapt = udf(capt)
    val Egender = udf(gend)

    val e1 = empldf.withColumn("ename",ENameCapt(empldf("ename")))
    e1.show()

    val e2 = e1.withColumn("egen",Egender(e1("egen")))
    e2.show()
  }
  def capt=(ename: String) => {
    var res0 = ename.toLowerCase.capitalize
    res0
  }

  def gend=(egen: String) => {
/*    if (egen=="m" || egen=="M")
      "Male"
    else if (egen=="f" || egen=="F")
      "Female"
    else
      "--"*/
    if (egen.toUpperCase=="M")
      "Male"
    else if (egen.toUpperCase=="F")
      "Female"
    else
      "--"
  }
}