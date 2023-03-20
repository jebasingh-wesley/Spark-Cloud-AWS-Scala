package cloud.aws

import org.apache.spark.sql._
//spark-submit --jars file:///home/hduser/postgresql-42.2.18.jar
// --class org.inceptez.spark.aws.rdsRedshiftToHive /home/hduser/rdsleanjar.jar
// Usecase 3 - Public cloud using EC2
object rdsRedshiftToS3 extends App {

    val spark = SparkSession.builder().appName("Usecase 3 - Public cloud using EC2").getOrCreate()
      //.master("local")


      spark.sparkContext.setLogLevel("INFO")
      
      
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAWUTLVBQQVMPLCPO3")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
     // spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      val lddt=args(0);
    //val lddt="25-DEC-21"
      val customquery=s"(select * from healthcare.drugs where loaddt='$lddt') tblquery"
      
      println(customquery)

      val dfdrugs = spark.read.format("jdbc").option("url", "jdbc:postgresql://database-1.cqeltditr1oc.us-east-1.rds.amazonaws.com:5432/dev").
                                                  option("driver", "org.postgresql.Driver").option("dbtable", customquery).option("user", "izuser").
                                                  option("password", "Inceptez123").load()
      println("Data from RDS Postgres")
      
      dfdrugs.cache();
      dfdrugs.show(5,false)
      dfdrugs.na.drop.createOrReplaceTempView("postgresdrugs")

       
      val dfpatients = spark.read.format("com.databricks.spark.redshift")
                                  .option("url", "jdbc:redshift://redshift-cluster-1.cdnd7lzbkma0.us-east-1.redshift.amazonaws.com:5439/dev?user=izuser&password=Inceptez123")
                                  .option("forward_spark_s3_credentials",true)
                                  //.option("jdbcdriver","org.postgresql.Driver")
                                  .option("dbtable", "patients").option("tempdir", "s3a://iz.databricks/tempdir/").load()
      println("Data from RedShift")

      dfpatients.cache()
      dfpatients.show(5,false)
      dfpatients.na.drop.createOrReplaceTempView("redshiftpatients")
      
      val widedataDF=spark.sql("""select d.*,p.* from postgresdrugs d inner join redshiftpatients p on d.uniqueid=p.drugid""")
      widedataDF.show(5,false)
      //widedataDF.write.mode("overwrite").partitionBy("loaddt").saveAsTable("default.patient_drugs_part")
      //println("Data loaded to On Premise Hive Table ")
      
       import org.apache.spark.sql.functions._;
       import spark.sqlContext.implicits._;
       
       
        val curts=Seq(1).toDF().withColumn("current_timestamp",current_timestamp).select(date_format(col("current_timestamp"),"yyyyMMddHHMMSS")).take(1)(0)(0).toString
        val s3location="s3a://iz.bucket1/rds_retail/patientdrugsinfo_"+curts
        widedataDF.write.option("mode", "overwrite").option("header","true").csv(s3location);
        println("S3 Object is created successfully")
        println("S3 location " + s3location)

    val s3df=spark.read.option("header","true").csv(s3location)
        s3df.show(5,false)

       
}


















