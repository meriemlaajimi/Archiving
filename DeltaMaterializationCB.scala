
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.io.Source.fromFile
import scala.io.Source.fromInputStream
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType};
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Try
import java.nio.file.{ Files, Paths }
import org.apache.spark.sql.hive.HiveContext;
import java.io.File


 case class Auction(Subject: Option[String], Predicate: Option[String],Object:Option[String])
object FullMaterialization {
  def main(args: Array[String]) {
   
    
   val  sparkConf = new SparkConf().setAppName("SimpleApp");
   val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val warehouseLocation = new File("spark-warehouse").getAbsolutePath
   val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
 .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()
  


import sqlContext.implicits._

import org.apache.spark.sql._


import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};
//Mapping  RDF files.nt  to  DataFrame
val auction1 = sc.textFile("s3://dataversionsarchiving/data/1.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
//Adding a version tag
val df1=auction1.withColumn("version",lit("v1"))



val auction2 = sc.textFile("s3://dataversionsarchiving/data/5.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df2=auction2.withColumn("version",lit("v5"))

val auction3 = sc.textFile("s3://dataversionsarchiving/data/10.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df3=auction3.withColumn("version",lit("v10"))

val auction4 = sc.textFile("s3://dataversionsarchiving/data/15.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df4=auction4.withColumn("version",lit("v15"))

val auction5 = sc.textFile("s3://dataversionsarchiving/data/20.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df5=auction5.withColumn("version",lit("v20"))

val auction6 = sc.textFile("s3://dataversionsarchiving/data/25.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df6=auction6.withColumn("version",lit("v25"))

val auction7= sc.textFile("s3://dataversionsarchiving/data/30.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df7=auction7.withColumn("version",lit("v30"))

val auction8 = sc.textFile("s3://dataversionsarchiving/data/35.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df8 =auction8.withColumn("version",lit("v35"))

val auction9 = sc.textFile("s3://dataversionsarchiving/data/40.nt").map(_.split(" ")).map(p => 
Auction(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df9=auction9.withColumn("version",lit("v40"))



val res=df1.union(df2)
val res1 =res.union(df3)
val res2 =res1.union(df4)
val res3 =res2.union(df5)
val res4 =res3.union(df6)
val res5 =res4.union(df7)
val res6 =res5.union(df8)
val res7 =res6.union(df9)


res7.write.mode(SaveMode.Append).saveAsTable("TableWithoutPartitions")

//Mapping  RDF files.csv to  DataFrame
val auction10 = sc.textFile("s3://dataversionsarchiving/addingv1v5.csv").map(_.split(",")).map(p => 
Auctionm(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df10=auction10.withColumn("version",lit("A"))

val auction11 = sc.textFile("s3://dataversionsarchiving/supprimv1v5.csv").map(_.split(",")).map(p => 
Auctionm(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df11=auction11.withColumn("version",lit("D"))

val auction12 = sc.textFile("s3://dataversionsarchiving/addingv1v10.csv").map(_.split(",")).map(p => 
Auctionm(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df12=auction12.withColumn("version",lit("A1"))

val auction13 = sc.textFile("s3://dataversionsarchiving/supprimv1v10.csv").map(_.split(",")).map(p => 
Auctionm(Try(p(0).toString()).toOption,Try(p(1).toString()).toOption,Try(p(2).toString()).toOption)).toDF()
val df13=auction13.withColumn("version",lit("D1"))




 
 
df10.write.mode(SaveMode.Append).saveAsTable("ADD")
df11.write.mode(SaveMode.Append).saveAsTable("DELETE")
df12.write.mode(SaveMode.Append).saveAsTable("ADD1")
df13.write.mode(SaveMode.Append).saveAsTable("DELETE1")


 val now = System.nanoTime
//Materialize V5

val res8= sqlContext.sql("SELECT Subject,Predicate,Object from TableWithoutPartitions Where version ='v1' ")
val res9=sqlContext.sql("SELECT Subject,Predicate,Object from ADD  ")
val res10=res8.union(res9)
res10.write.mode(SaveMode.Append).saveAsTable("table")



val df17=sqlContext.sql("SELECT Subject,Predicate,Object from table  MINUS SELECT Subject,Predicate,Object from delete").write.saveAsTable("tableV5")

//Materialize V10

val res14= sqlContext.sql("SELECT Subject,Predicate,Object from TableWithoutPartitions Where version ='v1' ")
val res15=sqlContext.sql("SELECT Subject,Predicate,Object from ADD1  ")
val res16=res8.union(res9)
res16.write.mode(SaveMode.Append).saveAsTable("table1")



val df18=sqlContext.sql("SELECT Subject,Predicate,Object from table1  MINUS SELECT Subject,Predicate,Object from delete1").write.saveAsTable("tableV10")
//Difference between v5 and v10
val df18=sqlContext.sql("SELECT Subject,Predicate,Object from tableV5  MINUS SELECT Subject,Predicate,Object from tableV10 ").write.saveAsTable("DELETEv1v5")



val df19=sqlContext.sql("SELECT Subject,Predicate,Object from tableV10 MINUS SELECT Subject,Predicate,Object from tableV5 ").write.saveAsTable("ADDv1v5")


val micros = (System.nanoTime - now) / 1000
println("%d microseconds".format(micros))





 

}}
