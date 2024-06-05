import requests
import logging
from google.cloud import bigquery
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class AccessTokenManager:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_info = {
            'token': None,
            'expires_at': datetime.now()
        }
        self.lock = threading.Lock()
        self.refresh_access_token_if_needed()  # Initial token fetch

    def get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        response = requests.post(url, headers=headers, data=body)
        response.raise_for_status()
        token_data = response.json()
        return token_data['access_token'], datetime.now() + timedelta(seconds=token_data['expires_in'] - 300)

    def refresh_access_token_if_needed(self):
        with self.lock:
            if datetime.now() >= self.token_info['expires_at']:
                access_token, expires_at = self.get_access_token()
                self.token_info['token'] = access_token
                self.token_info['expires_at'] = expires_at

    def get_token(self):
        with self.lock:
            return self.token_info['token']

def get_all_roles(access_token):
    roles_url = "https://graph.microsoft.com/v1.0/directoryRoles"
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    response = requests.get(roles_url, headers=headers)
    response.raise_for_status()
    roles = response.json().get('value', [])
    return {role['id']: role['displayName'] for role in roles}

def get_user_roles(access_token, user_id, all_roles):
    user_roles_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/memberOf"
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    try:
        response = requests.get(user_roles_url, headers=headers)
        response.raise_for_status()
        user_roles = response.json().get('value', [])
        user_role_names = [all_roles.get(role['id']) for role in user_roles if role.get('@odata.type') == '#microsoft.graph.directoryRole']
        return user_role_names
    except requests.exceptions.HTTPError as e:
        logging.error(f"An error occurred while fetching roles for user {user_id}: {e}")
        return None

def get_user_details(access_token, user_id):
    user_url = f"https://graph.microsoft.com/v1.0/users/{user_id}"
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    try:
        response = requests.get(user_url, headers=headers)
        response.raise_for_status()
        user = response.json()
        return user.get('displayName', 'Unknown')
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logging.warning(f"User with ID {user_id} not found.")
        else:
            logging.error(f"An error occurred while fetching details for user {user_id}: {e}")
        return None

def get_tenants_users_from_bigquery(client):
    query = """
        SELECT tenant_id, ARRAY_AGG(user_id) as user_ids 
        FROM `my_project.dataset_name.my_table` 
        WHERE PARSE_DATE('%Y-%m-%d', date_inserted) = (SELECT MAX(DATE(date_inserted)) FROM `my_project.dataset_name.my_table`)
        GROUP BY tenant_id
    """
    query_job = client.query(query)
    results = query_job.result()  # Waits for job to complete

    tenants_users = []
    for row in results:
        tenants_users.append({'tenant_id': row['tenant_id'], 'user_ids': row['user_ids']})
    return tenants_users

def insert_data_into_bigquery(client, dataset_name, table_name, rows_to_insert):
    table_id = f"{client.project}.{dataset_name}.{table_name}"
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logging.error(f"Errors occurred while inserting rows: {errors}")

def process_user(tenant_id, user_id, access_token, all_roles):
    try:
        user_display_name = get_user_details(access_token, user_id)
        if user_display_name is None:  # Skip user if details couldn't be fetched
            logging.warning(f"User details not found for user ID: {user_id}")
            return None

        user_role_names = get_user_roles(access_token, user_id, all_roles)
        if user_role_names is None:  # Skip user if roles couldn't be fetched
            logging.warning(f"User roles not found for user ID: {user_id}")
            return None

        # Return the new row of data for the user
        return {
            'tenant_id': tenant_id,
            'user_id': user_id,
            'display_name': user_display_name,
            'roles': ', '.join(user_role_names),
            'current_date': datetime.now().date().isoformat()  # Current date in ISO format (YYYY-MM-DD)
        }
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error while processing user {user_id}: {e}")
        return None  # Skip this user and continue with others

def process_tenant(tenant_user, client_id, client_secret, destination_dataset_name, destination_table_name, bq_client):
    tenant_id = tenant_user['tenant_id']
    logging.info(f"Processing tenant: {tenant_id}")

    # Initialize access token manager for this tenant
    token_manager = AccessTokenManager(tenant_id, client_id, client_secret)

    # Refresh the token before using it
    token_manager.refresh_access_token_if_needed()
    access_token = token_manager.get_token()
    all_roles = get_all_roles(access_token)  # Get all roles for the tenant

    # Process users in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=300) as executor:
        user_futures = {
            executor.submit(process_user, tenant_id, user_id, access_token, all_roles): user_id
            for user_id in tenant_user['user_ids']
        }
        
        rows_to_insert = []
        for future in as_completed(user_futures):
            user_id = user_futures[future]
            try:
                user_data = future.result()
                if user_data:  # Only append if user_data is not None
                    rows_to_insert.append(user_data)
                    logging.info(f"Processed user {user_data['user_id']} for tenant {tenant_id}")
            except Exception as e:
                logging.error(f"An error occurred while processing user {user_id} for tenant {tenant_id}: {e}")

    # Insert data into BigQuery for the current tenant
    if rows_to_insert:
        logging.info(f"Inserting data for tenant {tenant_id} into BigQuery")
        insert_data_into_bigquery(bq_client, destination_dataset_name, destination_table_name, rows_to_insert)
        logging.info(f"Data inserted for tenant {tenant_id}")
    else:
        logging.info(f"No data to insert for tenant {tenant_id}")

# Main part of the script
if __name__ == "__main__":
    # Client credentials (move to a secure place)
    client_id = '<client_id>'
    client_secret = '<client_secret>'

    # Destination BigQuery details
    destination_dataset_name = 'dataset_name'
    destination_table_name = 'all_azure_user_roles'

    # Initialize BigQuery client
    bq_client = bigquery.Client(project='my_project')

    # Get tenant and user information from BigQuery
    tenants_users = get_tenants_users_from_bigquery(bq_client)

    # Check if tenants_users is not empty
    if not tenants_users:
        logging.error("No tenants or users found in BigQuery.")
        exit(1)

    # Use ThreadPoolExecutor to process tenants in parallel
    logging.info("Starting data processing for all tenants")
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        tenant_futures = {
            executor.submit(process_tenant, tenant_user, client_id, client_secret, destination_dataset_name, destination_table_name, bq_client): tenant_user['tenant_id']
            for tenant_user in tenants_users
        }

        # Wait for all threads to complete
        for future in as_completed(tenant_futures):
            tenant_id = tenant_futures[future]
            try:
                future.result()  # This will also raise any exceptions caught during the thread execution
                logging.info(f"Completed processing for tenant {tenant_id}")
            except Exception as e:
                logging.error(f"An error occurred while processing tenant {tenant_id}: {e}")

    logging.info("Data insertion for all tenants completed.")
