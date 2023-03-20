# spark_Cloud.AWS_Scala
api exposed by AWS to hit their s3 buckets.load public cloud RDS,REDSHIFT to private cloud HIVE TABLE.

Usecase 2 - load public cloud RDS,REDSHIFT to private cloud HIVE TABLE
Ensure to change the postgres and redshift connection urls
Ensure to run the writeToRedShift program to load data to redshift and run the create/insert script into RDS table.
Ensure to change the loaddt to today.




source sys -> DSA -> DWH(ODS) -> Marts				
				
If i want to load live transactional data for transaction purpose (pos, atm transactions..) -> 
RDS (load will happen as singular transactions as ins/upd/delete)
If i want to load batch transactional data for historical analysis purpose (pos, atm transactions..) 
-> Redshift (load will happen as bulk/dump load as ins/upd/delete)
				
