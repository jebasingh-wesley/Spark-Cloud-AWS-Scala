package cloud.aws

import org.apache.spark.sql._

//spark-submit --jars file:///home/hduser/postgresql-42.2.18.jar --master spark://localhost:7077
// --class org.inceptez.spark.aws.emrRdsToS3 /home/hduser/emrRdsToS3leanjar.jar

object emrRdsToS3 extends App {

    val spark = SparkSession.builder().appName("EMR RDS to S3 Public Cloud").getOrCreate()
      //.master("local")
      spark.sparkContext.setLogLevel("ERROR")

      val lddt=args(0);
      //val lddt="25-DEC-21"

      val customquery=s"(select * from healthcare.drugs where loaddt='$lddt') tblquery"
      val dfdrugs = spark.read.format("jdbc").option("url", "jdbc:postgresql://database-1.cqeltditr1oc.us-east-1.rds.amazonaws.com:5432/dev").
                              option("driver", "org.postgresql.Driver").option("dbtable", customquery).option("user", "izuser").
                                  option("password", "Inceptez123").load()


     
      dfdrugs.cache();
      dfdrugs.na.drop.createOrReplaceTempView("postgresdrugs")
           
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAWUTLVBQQVMPLCPO3")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
      
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com") // api exposed by AWS to hit their s3 buckets.
      spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true") //non blocking http authentication once for all to be done
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") // s3 adv/accelerated feature to store upto 5TB of multipard data into S3
       
  
       import org.apache.spark.sql.functions._;
       import spark.sqlContext.implicits._

    val curts=Seq(1).toDF().withColumn("current_timestamp",current_timestamp).select(date_format(col("current_timestamp"),"yyyyMMddHHMMSS")).take(1)(0)(0).toString
       dfdrugs.write.option("mode", "overwrite").csv("s3a://iz.bucket1/rds_retail/drugsinfo_"+curts);

  println("S3 Object is created successfully")

       
}