import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

object sparkfilemysql extends App{
  
  val spark = SparkSession.builder().master("local[*]")
  .appName("example")
  .getOrCreate()
  
  import spark.implicits._
  
  val sch = StructType(List(
      StructField("order_id", DoubleType, true),
      StructField("order_date", DoubleType, true),
      StructField("order_customer_id", DoubleType, true),
      StructField("order_status", StringType, true)
      
    ))
  
  val df = spark.read.option("header",true)
  .schema(sch)
 .csv("/home/cloudera/Desktop/hemant/orders.csv")
 
 val resultDf = df.select("order_date", "order_customer_id", "order_status")
 
 
 val url = """jdbc:mysql://localhost:3306/bigdataproject"""
 
 resultDf.write
 .format("jdbc")
          .option("driver","com.mysql.jdbc.Driver")
          .option("url", url)
          .option("dbtable", "heman")
          .option("user", "root")
          .option("password", "cloudera")
          .mode("append")
          .save()
 
 

  
  df.printSchema()
  
}