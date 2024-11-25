# Apache Airflow ETL Project

This project demonstrates an **ETL (Extract, Transform, Load)** pipeline built using **Apache Airflow** to process toll data. The pipeline extracts data from multiple file formats (CSV, TSV, and fixed-width), performs transformations, and consolidates the data into a final output.

---

## Project Overview

The goal of this project is to automate the ETL process using Apache Airflow. The tasks performed by the pipeline are as follows:

1. **Extract**:
   - Extract data from different file formats:
     - **CSV** (Vehicle Data)
     - **TSV** (Toll Plaza Data)
     - **Fixed-width** (Payment Data)
   
2. **Transform**:
   - Merge the extracted data into a single CSV file.
   - Convert all data values to uppercase to standardize the format.
   
3. **Load**:
   - Store the final transformed data into a new CSV file.

---

## Project Structure

```plaintext
/home/project/
├── airflow/
│   ├── dags/
│   │   └── ETL_toll_data.py              # Airflow DAG script for ETL pipeline
│   └── airflow.cfg                       # Airflow configuration file
├── finalassignment/
│   └── staging/                          # Raw and processed data files
│       ├── vehicle-data.csv              # Sample CSV data
│       ├── tollplaza-data.tsv           # Sample TSV data
│       ├── payment-data.txt             # Sample fixed-width data
│       └── transformed_data.csv         # Final output after transformation

```


### Project Workflow
1. Environment Setup
The first step was setting up Apache Airflow in a local environment. This involved installing the necessary dependencies and configuring the airflow.cfg file to ensure that Airflow would recognize the correct dags folder for the project.

2. Creating the ETL DAG
A new DAG script was created in the /dags/ directory. This script defined an ETL pipeline that utilized Apache Airflow’s BashOperator to execute bash commands for the ETL tasks.
The DAG was structured to process raw data in several steps:
Unzipping Data: Extracting the .tgz archive containing raw data files.
Extracting Data: Using the cut command to extract relevant columns from CSV, TSV, and fixed-width files.
Consolidating Data: Merging the extracted data into a single file using the paste command.
Transforming Data: Applying data transformation using awk to convert all text in the final dataset to uppercase.

3. Configuring Task Dependencies
The tasks were ordered to execute sequentially by defining their dependencies using the >> operator. This ensured that each task would only execute once its predecessor had successfully completed.

4. Testing and Debugging the DAG
The DAG was tested by triggering it manually through the Airflow UI to ensure that all tasks were executing as expected and in the correct order.
Logs were reviewed to identify any issues, and necessary adjustments were made to the tasks or DAG configuration.

5. Execution and Monitoring
The DAG was set to run on a daily schedule (@daily), with the catchup parameter set to False to prevent backfilling.
The Airflow UI was used to monitor the execution of the DAG, check task status, and review logs for any errors or performance issues.

6. Final Output
Upon successful execution, the final transformed data was stored in the specified output directory. This data was now ready for analysis or further processing.
