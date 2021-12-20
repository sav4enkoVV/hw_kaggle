import org.apache.spark.sql.functions._

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    println("Hello, it's my homework!")

    val source = "data.csv"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Homework")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val df = spark
      .read
      .option("header", true)
      .csv(source)

    df.show()
    df.printSchema()

    val EngineHP = df
      .groupBy("Make", "Year")
      .agg(
        avg("Engine HP").as("Average_Engine_HP"),
        min("Engine HP").as("Minimum_Engine_HP"),
        max("Engine HP").as("Maximum_Engine_HP")
      )
      .select("Make", "Year", "Average_Engine_HP", "Maximum_Engine_HP", "Minimum_Engine_HP")
      .sort(desc("Year"))
      .show()

    spark.stop()
  }
}