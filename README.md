# Netflix Data ETL Project

## Overview
This project focuses on the extraction, transformation, and loading (ETL) of Netflix movie data from a CSV file into an Oracle database. The data undergoes various cleaning processes to ensure its quality before being stored in the target database.

## Repository Contents
- **ETL_NETFLIX_DATA_CODE/**: Python script that handles data transformation and loading processes.
- **data_netflix/**: Source data in CSV format downloaded from Kaggle.
- **etl_netflix/**: SQL queries used throughout the project for data manipulation and validation.
- **project_diagram/**: Diagram illustrating the flow of the ETL process.
- **netflix_dataset_cleaning/**: Jupyter Notebook used for data cleaning and exploration.
- **netflix_data_pipline_documentatio/**: PDF which contain a documentation for the roject

## ETL Process
1. **Data Transfer**: The data is transferred from the CSV file to the src_netflix_data staging table in Oracle using Informatica PowerCenter. A total of 18,860 records were successfully transferred.
  
2. **Data Transformation**: The following transformations were performed using Python:
   - Removal of null values.
   - Change of data types to ensure consistency.
   - Formatting of string columns.
   - Ordering of data based on specific criteria.
   
   After these transformations, the dataset was reduced to 15,134 records.

3. **Data Loading**: The cleaned data is then moved to the tgt_netflix_cleaned_data table in the Oracle database.

## Testing
- **Insert Process**: A test insert was performed by adding a new row to the source table. The inserted record appeared successfully in the target table after running the ETL process.
  
- **Update Process**: An update was made to the inserted record, and upon running the code again, the changes were reflected in the target table.

## Getting Started
To run this project, ensure you have the following:
- Python 3.x installed.
- Required Python libraries (e.g., pandas, SQLAlchemy).
- Access to an Oracle database and Informatica PowerCenter for data transfer.

