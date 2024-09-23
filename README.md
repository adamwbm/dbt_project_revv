
# Financial Ledger - DBT Project

## Overview
This project uses DBT (Data Build Tool) to consolidate financial data into a Kimball-style dimensional model to ultimately build a financial ledger for both the plan and reports subscription lines of business. The pipeline consists of 4 layers:
- **Staging Layer**: Raw data from seed files is cleaned and normalized with lightweight transformations and joins to prepare the data for further processing. This layer is constructed as views to reduce resource usage and complexity.
- **Intermediate Layer**: This layer is used to help keep the mart clean and abstract away heavy computation. Currently it is holding 2 ledger tables in anticipation of a single consolidated ledger built within the mart table. If it's decided that they are best kept separate, they can be moved to the mart as fact tables.
- **Marts Layer**: The models in the marts layer are materialized as tables, representing business-ready data optimized for reporting and analytics.
- **Reports Layer**: The reports to end users are contained in the reports folder and built as views. If these views begin to take too long to build for end users as the data scales, these should be converted to tables.


## Challenges and Future Considerations
The first main challenges I encountered was determining how to tie plan data to report data, since they are two different products with different granularities. I kept them separate for this exercise as intermediate tables, but in a real world situation, I would talk to the business and assess the level of scale to determine if these could better be handled by a factless fact table using the dim_calendar table as the base to join both plan and report data on date.

The other challenge was fully understanding the raw data. I made some assumptions about MoQ and tier usage data that might explain the discrepancies in the final results. In a real world scenario, I would rely on communication with a product manager or engineer to clear up ambiguity.

There is plenty of documentation, cleanup and optimization that could be done going forward, but U considered this out of scope for the project. If I had additional time, I would improve the column namings and add comments to my code and perhapse use macros and additonal CTEs to improve the code modularity.

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
To create a local PostgreSQL instance using Docker, run the following command:
```bash
docker run --name postgres \
    -e POSTGRES_USER=myusername \
    -e POSTGRES_PASSWORD=mypassword \
    -p 5432:5432 \
    -v /your/local/path:/var/lib/postgresql/data \
    -d postgres
```
Make sure to replace `/your/local/path` with the actual directory on your machine where you'd like to store the PostgreSQL data.


### Running the Project
Once the environment is set up, run the project with:
```bash
dbt run
```

This will execute all transformations defined in the dbt models and materialize the marts as tables in your PostgreSQL instance.