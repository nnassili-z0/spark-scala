package ma.naoufanassili.sparkscalastuff

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-test")
      .master("local[*]")
      // Ensure the driver advertises a reachable host on Windows to executors
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true )
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    df.select("Date", "Open", "Close").show()

    // col("Date")
    import spark.implicits._
    // $"Date"

    df.select(col("Date"), $"Open", df("close")).show()

    val column = df("Date")
    df.select(column, $"Open", df("close")).show()
  }
}

