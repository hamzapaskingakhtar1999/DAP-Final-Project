# DAP-Final-Project
**ETL Process Implementation using Luigi and Dagster**

**Overview**

This project demonstrates the implementation of Extract, Transform, Load (ETL) processes for data analysis using Luigi and Dagster, popular Python packages for workflow management and orchestration. The project uses three datasets, two from TMDb website (semi-structured data in JSON format) and one from Kaggle website (structured data in CSV format).

**Datasets**

- Two datasets extracted from TMDb website using API key (semi-structured data in JSON format)
- One dataset from Kaggle website (structured data in CSV format)

**ETL Process**

- Case 1: Luigi is used to perform ETL and load data into PostgreSQL database on localhost. The stored data is then used for machine learning algorithm to build a linear regression model.
- Case 2: Luigi is used to develop ETL process and store data in AWS Amazon Relational Database Service (RDS). The dataset is then retrieved for building linear regression model.
- Case 3: Preprocessed structured dataset stored in PostgreSQL is extracted, transformed, and loaded into Amazon RDS. The stored datasets from Case 2 and Case 3 are joined using inner join operation in PostgreSQL.

**Results**

- The implementation of ETL process using Luigi and Dagster proved to be successful.
- The joined dataset is used for developing linear regression model and making predictions.

**Technologies Used**

- Luigi
- Dagster
- PostgreSQL
- MongoDB
- AWS Amazon RDS
- Python
