# Project: Data Modeling with Postgres

## Summary

This project builds a Postgres-based relational data model and ETL pipeline for a music streaming startup, Sparkify. The goal is to organize and process JSON log and song metadata files so analysts can run queries to understand user behavior and song play patterns.

## How to Run the Scripts

1. **Run Table Creation Script:**

   ```bash
   python create_tables.py
   ```

   This script:

   * Connects to the default database.
   * Drops and recreates the `sparkifydb` database.
   * Drops any existing tables in `sparkifydb`.
   * Creates five tables using the star schema design.

2. **Run the ETL Script:**

   ```bash
   python etl.py
   ```

   This script:

   * Extracts song and log data from local directories.
   * Transforms the data (e.g., parsing timestamps, rounding durations).
   * Loads the data into the five database tables.

## File Descriptions

* **create\_tables.py**: Initializes the database and creates all required tables.
* **etl.py**: Executes the full ETL process (Extract, Transform, Load).
* **sql\_queries.py**: Stores SQL queries for table creation, dropping, inserting, and selection.
* **etl.ipynb**: Notebook used during development to iteratively test the ETL logic.
* **test.ipynb**: Sanity check notebook that runs queries against the final database to validate correct implementation.
* **data/**: Contains JSON files used in the ETL process.
* **README.md**: Overview and instructions for the project (this file).

## Schema Design

The database uses a star schema with the following tables:

* **Fact Table:**

  * `songplays`: Records for each song play event.

* **Dimension Tables:**

  * `users`: Information about the app users.
  * `songs`: Song metadata.
  * `artists`: Artist metadata.
  * `time`: Timestamps of song plays split by units (hour, day, etc.).

## Key Notes

* The songplays table may have many `None` values for `song_id` and `artist_id` because only one match exists in the dataset.
* `ON CONFLICT` clauses are used for upserts where appropriate.
* Duration rounding is used for accurate joins between song and log data.

## Author

Ar'eayla Jeanpierre
Student, Western Governors University
Data Analytics Program
