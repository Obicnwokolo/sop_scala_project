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
    val csvFilePath = "C:\\Users\\chigb\\Downloads\\fraud_Test.csv"

    // Read CSV file into a DataFrame
    val df = spark.read
      .option("header", "true") // Use the first row as header
      .option("inferSchema", "true") // Infer data types
      .csv(csvFilePath)
      .repartition(4)

    // Show the DataFrame (optional)
    df.show(5)

    // Define JDBC connection parameters
    val jdbcUrl = "jdbc:postgresql://18.132.73.146:5432/testdb"
    val dbProperties = new java.util.Properties()
    val dbTable = "sop_fraud_scala"
    dbProperties.setProperty("user", "consultants")  // Your database username
    dbProperties.setProperty("password", "WelcomeItc@2022")  // Your database password
    dbProperties.setProperty("driver", "org.postgresql.Driver")

    println("read successful")
    //df.show(5)
    val totalRows = df.count()
    println(s"Total number of rows: $totalRows")

    // Retrieve the maximum id from the database
    val maxIdQuery = "(SELECT MAX(id) AS max_id FROM sop_fraud_scala) AS temp_table"
    val maxIdDf = spark.read.jdbc(jdbcUrl, maxIdQuery, dbProperties)
    val maxId = maxIdDf.first().getAs[Number]("max_id").longValue()

    //------------------------------------------------------------------------------------------------
    // Filter the DataFrame
    val filteredDf = df.filter(df("id") > maxId)
    //filteredDf.show(5)
    val totalRows2 = filteredDf.count()
    println(s"new Total number of rows: $totalRows2")
    //-----------------------------------------------------------------------------------------------

    //filteredDf.write.mode("append").csv("C:\\Users\\chigb\\Downloads\\outputs")

    // Write DataFrame to PostgreSQL
    filteredDf.write //
      .mode("append") // Options: overwrite, append, ignore, error
      .jdbc(jdbcUrl, dbTable, dbProperties)

    println(s"$totalRows2 new records added successfully")


    // Stop SparkSession
    spark.stop()
  }
}
