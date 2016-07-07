/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql

// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

object SparkSqlExample {

  // $example on:create_ds$
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  case class Person(name: String, age: Long)
  // $example off:create_ds$

  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
        .builder()
        .appName("Spark SQL Example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    // $example off:init_session$

    runBasicDataFrameExample(spark)
    runDatasetCreationExample(spark)
    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    // $example on:create_df$
    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // age  name
    // null Michael
    // 30   Andy
    // 19   Justin
    // $example off:create_df$

    // $example on:untyped_ops$
    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // name
    // Michael
    // Andy
    // Justin

    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()
    // name    (age + 1)
    // Michael null
    // Andy    31
    // Justin  20

    // Select people older than 21
    df.filter(df("age") > 21).show()
    // age name
    // 30  Andy

    // Count people by age
    df.groupBy("age").count().show()
    // age  count
    // null 1
    // 19   1
    // 30   1
    // $example off:untyped_ops$

    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // $example off:run_sql$
  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    // $example on:create_ds$
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    // $example off:create_ds$
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:schema_inferring$
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
        .textFile("examples/src/main/resources/people.txt")
        .map(_.split(","))
        .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
        .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF
        .map(teenager => "Name: " + teenager(0))

    // or by field name
    teenagersDF
        .map(teenager => "Name: " + teenager.getAs[String]("name"))
        .show()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF
        .map(teenager => teenager.getValuesMap[Any](List("name", "age")))
        .show()
    // Map("name" -> "Justin", "age" -> 19)
    // $example off:schema_inferring$
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val people = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Row.
    import org.apache.spark.sql.Row

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{StructType, StructField, StringType}

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = people
        .map(_.split(","))
        .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDataFrame = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDataFrame.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val results = spark.sql("SELECT name FROM people")

    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    // $example off:programmatic_schema$
  }
}
