# Google Cloud Storage JSON files to BigQuery Table

## Install required packages

```bash
pip install -r requirements.txt
```

## Project Structure
* configs - contains config settings
* logs - contains script run logs
* repositories - contains Google Cloud Storage and BigQuery repositories
* services - determines how repositories work together(data transfer, error handling, JSON files parser)
* main.py - script exec file
## Usage

To delete, create and repopulate table use this command line:
```bash
$ py main.py -ie=replace 
```
-ie - if_exists parameter <br>
Important: this project is missing application_default_credentials.json for google auth. Please use your  
credentials

## Development Plan (updated 2023-12-10)
* Set up and finish logging
* Add Unit tests
* Not uploaded or unread files handling and checking
* Incremental insert, according blob.time_created parameter
* Check for performance improvement
* Calculate indexes

