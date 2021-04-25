# ETL project with Airflow managing the steps.

The objective of this project is to prepare a fact table so that the data analysis team can take the necessary metrics in relation to the musical preference of each client. The data was divided into two types of files, staging_song which contains the music information and staging_event which contains the user information and the way the application is used.

This project consists of the following steps

- transfer music and event data from S3 to the staging_song and staging_event tables on the redshift;
- create the fact table (songplays) using the staging_song and staging_event tables;
- create the songs, users, artists and time dimension tables;
- check that all dimension tables have been loaded.


![](./fluxo-dag.png)