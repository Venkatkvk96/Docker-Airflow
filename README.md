#**Daily Training Data Pipeline Automation for Induction and Regular Training - with Apache Airflow, Docker and MySQL.**



This project runs an **ETL pipeline** using **Apache Airflow** orchestrated with **Docker Compose**, with **MySQL** as the backend database.

The pipeline automatically extracts training data from CSV files, transforms it, and loads it into a MySQL table. This approach is used to automate the daily loading of induction and other regular training data across all shifts.




##**Project Structure:**



Airflow-docker:

> DAGS

	> daily_training_etl_uploads.py.

> .env

> docker-compose.yml

> Dockerfile

> requirements.txt

> training_files

	> Training_Details.csv ----------> # Uploading folder


##**Tech Stack:**

Python: ETL and Automation
Apache Airflow: Workflow orchestration (DAGs for ETL pipeline)
MySQL: Database for storing training records
Docker & Docker Compose: Containerization and orchestration
Git & GitHub: Version control and collaboration
Email Reports: smtplib / email





