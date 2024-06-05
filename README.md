# Azure
 
# Azure AD User Roles Processor

This Python script fetches user roles from Azure Active Directory (AD) and stores the data in Google BigQuery. It processes multiple tenants and their users in parallel, ensuring efficient data fetching and insertion.

## Features

- Retrieves user details and roles from Azure AD.
- Processes multiple tenants and their users concurrently.
- Stores the fetched data in Google BigQuery.
- Ensures thread-safe operations for token management.

## Requirements

- Python 3.7+
- Google Cloud SDK
- Azure AD Application with required permissions
- Service account with appropriate permissions for Google BigQuery

## Setup

### 1. Install Python Dependencies

```sh
pip install requests google-cloud-bigquery
```

### 2. Configure Google Cloud SDK

Ensure that the Google Cloud SDK is installed and configured with the appropriate service account.

```sh
gcloud auth activate-service-account --key-file=path/to/service-account-file.json
gcloud config set project your-gcp-project-id
```

### 3. Azure AD Application

Create an Azure AD application and obtain the following details:
- `client_id`
- `client_secret`
- `tenant_id`

The application should have the following API permissions:
- `Directory.Read.All`

## Configuration

Update the script with your Azure AD and Google Cloud details:

```python
client_id = '<your-client-id>'
client_secret = '<your-client-secret>'
destination_dataset_name = '<dest-bq-dataset-name-here>'
destination_table_name = '<dest-bq-table-name-here>'
```

## Usage

Run the script using Python:

```sh
python azure_ad_user_roles_processor.py
```

### Logging

The script uses Python's logging module to log information, warnings, and errors. Logs are displayed in the console with timestamps.

## Code Overview

### AccessTokenManager

Manages the access token for Azure AD API requests. Ensures thread-safe token refreshing.

### Functions

- `get_all_roles(access_token)`: Fetches all roles from Azure AD.
- `get_user_roles(access_token, user_id, all_roles)`: Fetches roles assigned to a user.
- `get_user_details(access_token, user_id)`: Fetches user details.
- `get_tenants_users_from_bigquery(client)`: Queries BigQuery to get tenant and user information.
- `insert_data_into_bigquery(client, dataset_name, table_name, rows_to_insert)`: Inserts data into BigQuery.
- `process_user(tenant_id, user_id, access_token, all_roles)`: Processes a single user.
- `process_tenant(tenant_user, client_id, client_secret, destination_dataset_name, destination_table_name, bq_client)`: Processes all users for a single tenant.

### Main Execution

- Initializes BigQuery client.
- Fetches tenant and user information from BigQuery.
- Processes each tenant in parallel using `ThreadPoolExecutor`.

## Error Handling

The script includes comprehensive error handling to log and skip any issues encountered during API requests or data processing.

## Contribution

Feel free to contribute to this project by opening issues or submitting pull requests.

## License

This project is licensed under the MIT License.