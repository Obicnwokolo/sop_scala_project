object initload_finance {
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



    // Write DataFrame to PostgreSQL
    df.write //
      .mode("append") // Options: overwrite, append, ignore, error
      .jdbc(jdbcUrl, dbTable, dbProperties)


    // Stop SparkSession
    spark.stop()
  }
}

