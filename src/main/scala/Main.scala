package ma.naoufanassili.sparkscalastuff

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-test")
      .master("local[*]")
      // Ensure the driver advertises a reachable host on Windows to executors
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true )
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()
  }
}

