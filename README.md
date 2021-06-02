# wikidata_export
Student project for script that extracts schema from Wikidata to assist a program in query auto-completion

## Running the script
To run the script first make sure you have all of the requirements.
Then the script can be simply run with 'Python'.
**Important: The script runs a really long time, up to 6 hours.**

## Requirements
* psycopg2 : Python library, used for PostgreSQL database connection
* requests : Python library, used for HTTP connections
* PostgreSQL Database and created sample schema(Can be created from 'sample-schema-creation.pgsql')
* properties.ini file, used for config, 'properties.ini.example' can be used as reference
