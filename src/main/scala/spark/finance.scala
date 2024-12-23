package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object finance {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("CSV to PostgresDb")
      .master("local[*]")
      .getOrCreate()

    // Define the file path
    val csvFilePath = "C:\\Users\\chigb\\Downloads\\financial_data.csv"

    // Read CSV file into a DataFrame
    val df = spark.read
      .option("header", "true") // Use the first row as header
      .option("inferSchema", "true") // Infer data types
      .csv(csvFilePath)

    // Show the DataFrame (optional)
    df.show(5)

    // Define JDBC connection parameters
    val jdbcUrl = "jdbc:postgresql://18.132.73.146:5432/testdb"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "consultants")  // Your database username
    dbProperties.setProperty("password", "WelcomeItc@2022")  // Your database password
    dbProperties.setProperty("driver", "org.postgresql.Driver")

    val dbTable = "sop_finace_scala"


    // Write DataFrame to PostgreSQL
    df.write
      .mode("overwrite") // Options: overwrite, append, ignore, error
      .jdbc(jdbcUrl, dbTable, connectionProperties)

    // Stop SparkSession
    spark.stop()
  }
}
