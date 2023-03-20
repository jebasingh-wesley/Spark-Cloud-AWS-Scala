package cloud.aws

import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream

object writeToRedShift extends App{

    val spark = SparkSession
														.builder()
														.master("local")
														.appName("s3read")
														//.config("hive.metastore.uris", "thrift://localhost:9083")
														//.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
														.enableHiveSupport()
														.getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

				val conn=new Properties()
				val propFile= new FileInputStream("/home/hduser/install/aws_workouts/connection.prop") 
				conn.load(propFile)

				/* Reading AWS connection credentials from property file */
				val accesskey=conn.getProperty("access") 
				val secretkey =conn.getProperty("secret") 

// source sys -> DSA -> DWH(ODS) -> Marts				
				
// If i want to load live transactional data for transaction purpose (pos, atm transactions..) -> 
// RDS (load will happen as singular transactions as ins/upd/delete)
// If i want to load batch transactional data for historical analysis purpose (pos, atm transactions..) 
// -> Redshift (load will happen as bulk/dump load as ins/upd/delete)
				
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accesskey)
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretkey)
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
				spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")      
      
				val dfpatients=spark.read.option("inferschema",true).option("header",true).
												csv("hdfs://localhost:54310/user/hduser/awsdata/patients.csv")
          
      dfpatients.write.format("com.databricks.spark.redshift")
      .option("url", "jdbc:redshift://redshift-cluster-1.cdnd7lzbkma0.us-east-1.redshift.amazonaws.com:5439/dev?user=izuser&password=Inceptez123")
      .option("forward_spark_s3_credentials",true)
      .option("dbtable", "patients").
      option("tempdir", "s3a://iz.databricks/tempdir/").
      mode("append").save()
// hdfs -> spark bulk -> s3 bulk-> redshift table   
// hdfs -> spark (3 billion inserts) -> redshift table
      
        /* import org.apache.spark.sql.functions._;
       import spark.sqlContext.implicits._;
    val curts=Seq(1).toDF().withColumn("current_timestamp",current_timestamp).select(date_format(col("current_timestamp"),"yyyyMMddHHMMSS")).take(1)(0)(0).toString
       dfpatients.write.option("mode", "overwrite").csv("s3a://iz.bucket1/rds_retail/custinfo_"+curts);
*/  
      println("Redshift table load completed")
       

       
}












