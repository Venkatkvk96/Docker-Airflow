# Daily Training Data Pipeline Automation for Induction and Regular Training - with Apache Airflow, Docker and MySQL.

This project runs an **ETL pipeline** using **Apache Airflow** orchestrated with **Docker Compose**, with **MySQL** as the backend database.

The pipeline automatically extracts training data from CSV files, transforms it, and loads it into a MySQL table. This approach is used to automate the daily loading of induction and other regular training data across all shifts.



## Project Structure:


Airflow-docker:

- DAGS
	- daily_training_etl_uploads.py.
- .env
- docker-compose.yml
- Dockerfile
- requirements.txt
- training_files
	- Training_Details.csv ----------> Uploading folder


## Tech Stack:

1. Python: ETL and Automation
2. Apache Airflow: Workflow orchestration (DAGs for ETL pipeline)
3. MySQL: Database for storing training records
4. Docker & Docker Compose: Containerization and orchestration
5. Git & GitHub: Version control and collaboration
6. Email Reports: smtplib / email


## Services:

|**Service**        |**Role**			|**Port**
|MySQL				|Database backend	|3307:3306
|airflow_web		|Airflow Web UI		|8080:8080
|airflow_scheduler	|Schedules DAGs		|	-


## ETL flow:

1. **Extract**: Reads CSV files from training_files/share_folder
2. **Transform**: Cleans and standardizes data
3. **Load**: Inserts into MySQL table







