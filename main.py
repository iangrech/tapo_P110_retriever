import os
import pickle
import base64
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import email
from email import policy
from email.parser import BytesParser
import pandas as pd
import psycopg2  # For PostgreSQL connection
import shutil  # For moving files
import configparser  # For reading the config file

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Load configuration from the config file
config = configparser.ConfigParser()
config.read('config.ini')

# PostgreSQL connection details
DB_HOST = config['postgresql']['host']
DB_PORT = config['postgresql']['port']  # PostgreSQL port
DB_NAME = config['postgresql']['database']
DB_USER = config['postgresql']['user']
DB_PASSWORD = config['postgresql']['password']

# Folder paths
ATTACHMENTS_DIR = config['folders']['attachments']
OUTSOURCE_DIR = config['folders']['outsource']
SQL_OUT_DIR = config['folders']['sqloutstmnts']
ARCHIVE_DIR = config['folders']['archive']  # Archive folder path from config

# Gmail label name
GMAIL_LABEL = config['gmail']['label']

# SQL template file path
#SQL_TEMPLATE_PATH = 'sql_template.txt'
SQL_TEMPLATE_PATH = '_INSERT_SQL_TEMPLATE.sql'


def authenticate_gmail():
    """Authenticate and return the Gmail API service."""
    creds = None
    # The file token.pickle stores the user's access and refresh tokens.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no valid credentials available, prompt the user to log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)


def list_emails_from_label(service, after_date):
    """List all emails from a specific label after a certain date."""
    # Convert the date to the required format
    after_date_str = after_date.strftime('%Y/%m/%d')
    query = f'label:{GMAIL_LABEL} after:{after_date_str}'

    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print('No emails found.')
    else:
        print(f'Found {len(messages)} emails:')
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
            process_email(msg)


def process_email(msg):
    """Process an email to extract attachments, read Excel files, save as CSV, and delete the attachment."""
    # Decode the raw email content
    msg_bytes = base64.urlsafe_b64decode(msg['raw'])
    email_message = BytesParser(policy=policy.default).parsebytes(msg_bytes)

    # Create folders if they don't exist
    os.makedirs(ATTACHMENTS_DIR, exist_ok=True)
    os.makedirs(OUTSOURCE_DIR, exist_ok=True)
    os.makedirs(SQL_OUT_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)  # Ensure the Archive folder exists

    # Extract the email date from headers
    email_date = None
    for header in email_message.items():
        if header[0] == 'Date':
            email_date = datetime.strptime(header[1], '%a, %d %b %Y %H:%M:%S %z').strftime('%Y%m%d')
            break

    # Iterate through email parts to find attachments
    for part in email_message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue

        # Extract the filename
        filename = part.get_filename()
        if filename and (filename.endswith('.xlsx') or filename.endswith('.xls')):  # Check if it's an Excel file
            print(f'Found Excel attachment: {filename}')

            # Save the attachment
            attachment_path = os.path.join(ATTACHMENTS_DIR, filename)
            with open(attachment_path, 'wb') as f:
                f.write(part.get_payload(decode=True))
            print(f'Saved attachment to: {attachment_path}')

            # Read the Excel file and save as CSV
            if email_date:
                csv_filename = f"{os.path.splitext(filename)[0]}_{email_date}.csv"
                csv_path = os.path.join(OUTSOURCE_DIR, csv_filename)
                if read_excel_and_save_as_csv(attachment_path, csv_path):
                    # Delete the attachment file after successful CSV creation
                    os.remove(attachment_path)
                    print(f'Deleted attachment file: {attachment_path}')

                    # Generate SQL file from the CSV file
                    generate_sql_from_csv(csv_path, SQL_OUT_DIR)


def read_excel_and_save_as_csv(excel_path, csv_path):
    """Read an Excel file and save its contents as a CSV file with formatted date columns and renamed columns."""
    try:
        # Read the Excel file using pandas
        df = pd.read_excel(excel_path)
        print(f'Reading Excel file: {excel_path}')

        # Format date columns to 'yyyy-MM-dd HH:mm'
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M')

        # Rename the first column to 'ReadingDate'
        if len(df.columns) > 0:
            df.rename(columns={df.columns[0]: 'ReadingDate'}, inplace=True)

        # Rename the second column based on specific conditions
        if len(df.columns) > 1:
            second_column = df.columns[1]
            if second_column == 'Energy Usage(kWh)':
                df.rename(columns={second_column: 'EnergyUsage_kWh'}, inplace=True)
            elif second_column == 'Power(W)':
                df.rename(columns={second_column: 'Power_W'}, inplace=True)

        # Save the DataFrame as a CSV file
        df.to_csv(csv_path, index=False)
        print(f'Saved CSV file to: {csv_path}')
        return True  # Indicate success
    except Exception as e:
        print(f'Error processing Excel file: {e}')
        return False  # Indicate failure


def generate_sql_from_csv(csv_path, sql_out_dir):
    """Generate SQL insert statements from a CSV file using a template and write them to a .SQL file."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        print(f'Reading CSV file: {csv_path}')

        # Determine the table name based on the column names
        if 'EnergyUsage_kWh' in df.columns:
            table_name = 'extractor.energyday'
            value_column = 'kwh'
        elif 'Power_W' in df.columns:
            table_name = 'extractor.powerday'
            value_column = 'watts'
        else:
            print(f'Unknown column structure in CSV file: {csv_path}')
            return

        # Generate SQL file path
        sql_filename = f"{os.path.splitext(os.path.basename(csv_path))[0]}.sql"
        sql_path = os.path.join(sql_out_dir, sql_filename)

        # Read the SQL template
        with open(SQL_TEMPLATE_PATH, 'r') as template_file:
            sql_template = template_file.read()

        # Generate INSERT statements using the template
        with open(sql_path, 'w') as sql_file:
            for index, row in df.iterrows():
                ts = row[0]  # Use row[0] for the first column (ReadingDate)
                value = row[1]  # Use row[1] for the second column (EnergyUsage_kWh or Power_W)
                sourcefile = os.path.basename(csv_path)

                # Replace placeholders in the template with actual values
                sql_statement = sql_template.format(
                    table_name=table_name,
                    sourcefile=sourcefile,
                    ts=ts,
                    value_column=value_column,
                    value=value
                )
                sql_file.write(sql_statement + '\n')

        os.remove(csv_path)

        print(f'Saved SQL file to: {sql_path}')
    except Exception as e:
        print(f'Error generating SQL file: {e}')


def execute_sql_files(sql_out_dir):
    """Execute all SQL files in the SQLOutStmnts folder and move them to the Archive folder."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,  # Use the port from the config file
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Iterate through all SQL files in the SQLOutStmnts folder
        for sql_file in os.listdir(sql_out_dir):
            if sql_file.endswith('.sql'):
                sql_path = os.path.join(sql_out_dir, sql_file)
                print(f'Executing SQL file: {sql_path}')

                # Read and execute the SQL file
                with open(sql_path, 'r') as f:
                    sql_commands = f.read()
                    cursor.execute(sql_commands)
                    conn.commit()

                # Move the executed SQL file to the Archive folder
                archive_path = os.path.join(ARCHIVE_DIR, sql_file)
                shutil.move(sql_path, archive_path)
                print(f'Moved SQL file to: {archive_path}')


        # Close the database connection
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'Error executing SQL files: {e}')


def main():
    # Authenticate and create the Gmail API service
    service = authenticate_gmail()

    # Specify the date after which you want to extract emails
    after_date = datetime.now() - timedelta(days=30)  # Emails from the last 30 days

    # List emails from the specified label after the specified date
    list_emails_from_label(service, after_date)

    # Execute all SQL files and move them to the Archive folder
    execute_sql_files(SQL_OUT_DIR)

    # Output 5 blank lines and '------ DONE -----' to the console
    print("\n" * 5)
    print("------ DONE -----")


if __name__ == '__main__':
    main()