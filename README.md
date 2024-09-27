# File Uploader Connector

This Python script processes a CSV file, splits it into chunks, and uploads these chunks to a specified gateway URL. It uses JSON configuration if available, otherwise relies on command-line arguments..

## Table of Contents

- [Project Overview](#project-overview)
- [Installation](#installation)
- [Usage](#usage)

## Project Overview

The `CsvUploader` class in `main.py` handles the following functionalities:
1. **Reads and processes a CSV file** into manageable chunks.
2. **Uploads chunks** to a remote server.
3. **Retries** uploads on failure with exponential backoff.
4. **Logs** operations using a rotating log file.

## Installation

1. **Install Python**
    https://www.python.org/downloads/

2. **Download the repo**

3. **Navigate to the Project Directory**
    ```bash
       cd connector-config-automation
    ```

4. **Install Dependencies**
    ```bash
       pip install -r requirements.txt
    ```

5. **Create Configuration file with name config.json in same folder as main.py**
    ```json
       {
           "header": 1,
           "baseUrl": "https://yourapi.example.com",
           "username": "your_username",
           "password": "your_password",
           "connectionUuid": "your_connection_uuid",
           "profileUuid": "your_profile_uuid",
           "envQualysUsernameProperty": "ENV_QUALYS_USERNAME",
           "envQualysPasswordProperty": "ENV_QUALYS_PASSWORD"
       }
    ```

## Usage

### Running the Script

**To run the script, use the following command:**
  ```bash
     cd connector-config-automation
     python3 main.py --header <header-line> --csvPath <csv-file-path> --baseUrl <base-url> --username <username> --password <password> --profileUuid <profile-uuid> --connectionUuid <connection-uuid>
   ```
**The above command will use configuration from created config.json file**

*If config.json is not provided use the following command line args:*

- `--header`: Line number of the header row (default: 1).
- `--csvPath`: Path to the CSV file (required).
- `--baseUrl`: Base apigateway Qualys URL for uploading chunks (required).
- `--username`: Qualys Username for authentication (optional, if you provide value for envQualysUsernameProperty property we will read it from env).
- `--password`: Qualys Password for authentication (optional, if you provide value for envQualysPasswordProperty property we will read it from env).
- `--connectionUuid`: Connection UUID (required, this will be available on connector UI).
- `--profileUuid`: Profile UUID (required, this will be available on connector UI).
- `--envQualysUsernameProperty`: Environment variable name for username (optional).
- `--envQualysPasswordProperty`: Environment variable name for password (optional).

  
**To use the script in other scripts:**
 ```python
    import start from connector-config-automation.main.py
    start(csv_path)
 ```
