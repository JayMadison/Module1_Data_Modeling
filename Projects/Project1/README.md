## Data Modeling with Postgres
Sparify is looking for a way to better understand user behavior, ultimately that will allow them to upsell more effectively as well as make better strategic deacons on what type of music does best on their platform. A DataMart star schema database was created as the star schema will allow for easy understanding for the analysts. 

For example, the log files have duplicated level and name information, by moving them to their own table any updates only need to be made in one place, vs on every log entry. 

The ETL pipeline will consume the json files that are already being generated and quickly load them into the sparkifydb. 

This has shown a data quality issue in that a very small number (1) of the songplays are linkable to artists, which will impact the quality of analysis. Sparkify should investigate better data governance policies to avoid this in the future. Luckily the ETL pipe can load additional song / artist data once it is extracted from Sparkify's systems. 

## Schema
#### Fact Table
**Songplays** 

The schema is centered around a fact table (songplays) which records user actions when they play songs on the app. There are 6820 records in this table. This table links to 4 dimension tables to make it easier to easily report on user behavior.

#### Dimension Tables
**Users**

This table contains information about the users of the service, including the level of service.  This table has 96 records. 

**Time**

This table contains a breakdown of time stamps by common groupings (year, month etc). This table has 6813 records. 

**Songs**

This table contains information on the songs available on the service and links to the artist table so it is easy to locate history on a specific artist. This table has 71 records. 

**Artists**

This table holds information on the music artists associated with the songs on the service. They are also linked to song plays so that data about which artists are most popular is easy to obtain. This table has 69 records.
