
# Financial Ledger - DBT Project

## Overview
This project uses dbt (Data Build Tool) to consolidate financial data into a Kimball-style dimensional model to ultimately build a financial ledger for both the plan and reports subscription lines of buisness. The pipeline consists of two layers:
- **Staging Layer**: Raw data from seed files is cleaned and normalized with lightweight transformations and joins to prepare the data for further processing. This layer is constructed as views to reduce resource usage and complexity.
- **Marts Layer**: The models in the marts layer are materialized as tables, representing business-ready data optimized for reporting and analytics.
- **Reports Layer**: The reports to end users are contained in the reports file and built as views. If these views begin to take too long to build for end users as the data scales, these should be converted to tables.


## Challenges and Future Considerations
The first main challenges I encountered was determining how to tie plan data to report data, since they are two different products with different granularities. I kept them separate for this exercise as intermediate tables, but in a real world situation, I would talk to the business and assess the level of scale to determine if these could better be handled by a factless fact table using the dim_calendar table as the base to join both plan and report data on date.

The other challenge was determining if I should include rolling sums or partitioned data by billing period in the reports ledger. I would chat with the buisness more about the future needs, but ultimately, I could probably get away with including both to keep options open.

There is plenty of documentation, cleanup and optimization that could be done going forward, but considered this out of scope for the project. If I had time, I would improve the column namings and add comments to my code and perhapse use macros and additonal CTEs to improve the code modularity.

## Project Structure
```bash
├── models/
│   ├── staging/
│   ├── marts/
│   └── reports/  
├── seed/
├── dbt_project.yml
├── README.md
└── requirements.txt
```

## Requirements
- Ensure Docker is installed on your machine. You can download it from: https://www.docker.com/products/docker-desktop

Ensure all required packages are installed by running the following:
```bash
pip install -r requirements.txt
dbt deps
```

The following dependencies are necessary for running the project. These are managed in the `requirements.txt` file:
- dbt-postgres
- PostgreSQL

To install additional dependencies, run:
```bash
pip install -r requirements.txt
```


### PostgreSQL Setup
To create a local PostgreSQL instance using Docker, run the following command (Make sure to replace `/your/local/path` with the actual directory on your machine where you'd like to store the PostgreSQL data):
```bash
docker run --name postgres \
    -e POSTGRES_USER=myusername \
    -e POSTGRES_PASSWORD=mypassword \
    -p 5432:5432 \
    -v /your/local/path:/var/lib/postgresql/data \
    -d postgres
```


### Running the Project
Once the environment is set up, run the project with:
```bash
dbt run
```

This will execute all transformations defined in the dbt models and materialize the marts as tables in your PostgreSQL instance.

## Additional Notes
- **Data Governance**: Staging models are kept lightweight and focus only on cleaning and standardizing the raw data. Any complex business logic or calculations are deferred to the marts layer.
- **Performance Considerations**: Tables in the marts layer are optimized for querying and reporting to ensure fast, reliable access to financial data. The use of views in the staging layer keeps the development process agile and resource-efficient.
