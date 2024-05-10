# COVID-19 Data Pipeline

## Introduction

This project implements an automated data pipeline for processing and visualizing COVID-19 data. It ingests data from a CSV file, stores it in HDFS, processes it using Hive, and generates reports for visualization with Power BI.

## Business Goals

- *Automate the pipeline workflow from data ingestion to visualization for the COVID dataset.*
  - Display the top 10 countries ranked by death rate on a map.
  - Display the top 10 countries ranked by testing rate on a map.
  - Show the top 10 ranking countries in testing rate on a pie chart.
  - Add a custom chart of your choice to the dashboard.

## Technical Approach

### Environment Setup

- Installed VMware Workstation Player and configured a Cloudera virtual machine.
- Used WinSCP, a secure file transfer client, to move data between my local computer and the Cloudera VM.

### Data Preparation and Processing

- Created necessary files within my Cloudera home directory using the command line interface.
- Developed a shell script named Load_COVID_TO_HDFS.sh to automate uploading COVID-19 data to HDFS (Hadoop Distributed File System).
- Employed Hive, a data warehouse tool, to create three tables:
  - A staging table to temporarily hold the dataset for selection.
  - An ORC table (optimized for efficient queries) partitioned by country for faster data retrieval.
  - A final table responsible for generating the report data used for visualizations.

### Job Scheduling with Oozie

- Scheduled the execution of both the shell script and Hive script using Oozie, a workflow and job orchestration system.

### Data Retrieval and Visualization

- Transferred the data from the Covid_final_output table in HDFS to my local directory within the Cloudera VM.
- Used WinSCP again to transfer the processed data file to my personal computer for further analysis.
- Created visualizations with Power BI that aligned with the defined business requirements, considering line and stacked column charts to illustrate the relationship between testing rates and death rates effectively.

## Challenges and Solutions

### Tools Setup

Encountered initial hurdles with setting up the Cloudera VM and associated tools. Overcame these challenges by seeking online resources, including search engines and video tutorials.

### Memory Constraints

Faced memory limitations while running a large Hive script. Addressed this by breaking down the INSERT queries into smaller queries, each handling data for a subset of 30 countries, which mitigated the memory constraints.

## Conclusion

The project successfully implemented an automated data pipeline for processing and visualizing COVID-19 data. Utilizing tools like Cloudera, Hive, Oozie, and Power BI, the pipeline efficiently ingests, processes, and generates reports for insightful visualizations, enabling informed decision-making.
