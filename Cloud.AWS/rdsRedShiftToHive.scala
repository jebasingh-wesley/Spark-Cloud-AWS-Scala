package cloud.aws

import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream



//spark-submit --jars file:///home/hduser/postgresql-42.2.18.jar
// --class org.inceptez.spark.aws.rdsRedshiftToHive /home/hduser/rdsleanjar.jar
// Usecase 2 - load public cloud RDS,REDSHIFT to private cloud HIVE TABLE
// Ensure to change the postgres and redshift connection urls
// Ensure to run the writeToRedShift program to load data to redshift and run the create/insert script into RDS table.
// Ensure to change the loaddt to today.

object rdsRedshiftToHive extends App {

    val spark = SparkSession
                            .builder()
                            .master("local")
                            .appName("load public cloud RDS,REDSHIFT to private cloud HIVE TABLE")
                            .config("hive.metastore.uris", "thrift://localhost:9083")
                            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                            .enableHiveSupport()
                            .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      
      
				val conn=new Properties()
				val propFile= new FileInputStream("/home/hduser/install/aws_workouts/connection.prop") 
				conn.load(propFile)



				/* Reading AWS connection credentials from property file */
				val accesskey=conn.getProperty("access") 
				val secretkey =conn.getProperty("secret")


				spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accesskey)
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretkey)
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
				spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
				spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")



      println("Connecting to PostGres")
      val lddt="26-Dec-21";


      val customquery=s"(select * from healthcare.drugs where loaddt='$lddt') tblquery"


      println(customquery)


      val dfdrugs = spark.read.format("jdbc").option("url", "jdbc:postgresql://database-1.cqeltditr1oc.us-east-1.rds.amazonaws.com:5432/dev").
                                              option("driver", "org.postgresql.Driver").option("dbtable", customquery).option("user", "izuser").
                                              option("password", "Inceptez123").load()
                                              println("Data from RDS Postgres")


      dfdrugs.cache();

       println("Data sample from Postgre")


      dfdrugs.show(5,false)  
      dfdrugs.na.drop.dropDuplicates().createOrReplaceTempView("postgresdrugs")


      println("Connecting to Redshift")
       
      val dfpatients = spark.read.format("com.databricks.spark.redshift")
                                .option("url", "jdbc:redshift://redshift-cluster-1.cdnd7lzbkma0.us-east-1.redshift.amazonaws.com:5439/dev?user=izuser&password=Inceptez123")
                                .option("forward_spark_s3_credentials",true)
                                //.option("jdbcdriver","org.postgresql.Driver")
                                .option("dbtable", "patients").option("tempdir", "s3a://iz.databricks/tempdir/").load()


      println("Data from RedShift")
      dfpatients.cache()
      println("Data sample from Redshift")
      dfpatients.show(5,false)
      dfpatients.na.drop.dropDuplicates().createOrReplaceTempView("redshiftpatients")


      val widedataDF=spark.sql("""select d.*,p.* from postgresdrugs d inner join redshiftpatients p on d.uniqueid=p.drugid""")
                      widedataDF.show(5,false)
                      widedataDF.write.mode("overwrite").partitionBy("loaddt").saveAsTable("default.patient_drugs_part")

      println("Data loaded to On Premise Hive Table ")


      spark.sql("""select * from patient_drugs_part""").show(5,false)
      
      println("Writing to Postgres")

      val prop=new java.util.Properties();
            prop.put("user", "izuser")
            prop.put("password", "Inceptez123")
            prop.put("driver","org.postgresql.Driver")


      widedataDF.write.mode("overwrite").
                  jdbc("jdbc:postgresql://database-1.cqeltditr1oc.us-east-1.rds.amazonaws.com:5432/dev","patient_drugs_part",prop)
                  println("Spark job is completed")


       
}

















