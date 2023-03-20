package cloud.aws

import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream
object s3ToHDFS extends App {
		val spark = SparkSession.builder().master("local").appName("s3readHDFSWrite")
				//.config("hive.metastore.uris", "thrift://localhost:9083")
				//.config("spark.sql.warehouse.dir", "/user/hive/warehouse")   
				//.enableHiveSupport()
				.getOrCreate()
				spark.sparkContext.setLogLevel("ERROR")


				/* Reading AWS connection credentials from property file */
				val conn=new Properties()
				val propFile= new FileInputStream("/home/hduser/install/aws_workouts/connection.prop") 
				conn.load(propFile)
				val accesskey=conn.getProperty("access") 
				val secretkey =conn.getProperty("secret") 

				spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accesskey)
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretkey)
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
				spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
				
				val df = spark.read.option("header","false")
																.option("delimiter", ",")
																.option("inferschema", "true")
																//.csv("s3a://iz-bucket2/profile/txns")
																.csv("s3a://com.iz.databucket/folder1/cust_data") // N Virginia region
																.toDF("id","name","age")
				df.show(false)

				df.write.mode("overwrite").option("header",true).csv("hdfs://localhost:54310/user/hduser/awss3dataset/");
        println("Data writtern to CSV file")
        
   
        val dfwrite = spark.read.option("header","false")
													 .option("delimiter", ",")
													 .option("inferschema", "true")
													 .csv("file:///home/hduser/hive/data/txns").
													 toDF("txnid","dt","cid","amt","category","product","city","state","transtype")
       dfwrite.show(false)
       
       dfwrite.write.option("mode", "overwrite").partitionBy("state").
								 csv("s3a://com.izbucket.datasets/trans-data2");
								 println("S3 Objects are writterned")
				

}