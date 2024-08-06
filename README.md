# Spark-Cloud-AWS-Scala

## Overview

This repository contains Scala scripts for integrating Apache Spark with AWS services to handle data loading and processing tasks. The primary focus is on loading data from public cloud services (RDS and Redshift) into private cloud Hive tables. The repository also includes details on configuring connections, handling transactional data, and executing data load operations.

### Use Case

**Use Case 2: Loading Data from Public Cloud (RDS and Redshift) to Private Cloud Hive Table**

1. **Objective**:
   - Load data from AWS public cloud services (RDS and Redshift) into Hive tables in a private cloud environment.
   - Ensure that connection URLs are properly configured for PostgreSQL (RDS) and Redshift.
   - Execute necessary scripts to handle data loading and transformations.

2. **Instructions**:
   - **Connection URLs**: Update the PostgreSQL and Redshift connection URLs in the scripts to match your environment.
   - **Load Data**:
     - Run the `writeToRedShift` program to load data into Redshift.
     - Execute the `create/insert` script to populate the RDS table.
   - **Date Configuration**: Ensure to update the `loaddt` variable to reflect today's date for accurate data loading.

### Data Flow

- **Source**: 
  - **Sys**: System data source.
  - **DSA**: Data Staging Area.
  - **DWH (ODS)**: Data Warehouse (Operational Data Store).
  - **Marts**: Data Marts.

- **Transactional Data**:
  - **Live Transactions** (e.g., POS, ATM transactions):
    - **RDS**: Data is loaded as singular transactions (insert/update/delete).
  - **Batch Transactions** (e.g., historical analysis for POS, ATM transactions):
    - **Redshift**: Data is loaded in bulk (insert/update/delete) for historical analysis.

### Repository Structure

- **RDS_Redshift_to_Hive.scala**:
  - Contains the main logic for loading data from RDS and Redshift into Hive tables.

- **writeToRedShift.scala**:
  - Script to load data into Redshift.

- **create_insert_RDS.scala**:
  - Script to create and insert data into RDS tables.

- **configurations**:
  - Update connection URLs and `loaddt` as per your environment and data requirements.

### Getting Started

1. **Set Up AWS Services**:
   - Ensure AWS services (RDS and Redshift) are properly configured and accessible.
   - Set up Hive in your private cloud environment.

2. **Update Connection Details**:
   - Modify the connection URLs for PostgreSQL (RDS) and Redshift in the scripts.
   - Adjust the `loaddt` variable to today's date or as needed.

3. **Execute Scripts**:
   - Run the `writeToRedShift` script to load data into Redshift.
   - Execute the `create/insert` script to manage data in RDS tables.
   - Use the `RDS_Redshift_to_Hive.scala` script to move data from public cloud services to Hive tables.

4. **Verify Data**:
   - Check Hive tables for the loaded data to ensure accuracy and completeness.

Feel free to explore the scripts and adapt them to your specific use cases and environment. If you have any questions or encounter issues, please open an issue in this repository.

Happy data processing!

---

This README provides a clear and structured guide for using the Scala scripts in the repository, detailing how to set up and execute the data loading tasks from public cloud services to a private cloud environment.
