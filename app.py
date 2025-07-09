import os
import json
from datetime import datetime, date, timezone, timedelta # Ensure date is imported if used
import pytz # For timezone conversion
import gspread
from oauth2client.service_account import ServiceAccountCredentials # For gspread legacy auth
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv # For local development
import time
import requests
import threading # To run the sync job in a background thread
from flask import Flask, request, jsonify # Import Flask components

# --- CONFIGURATION ---
load_dotenv() # Load environment variables from .env file for local development

# --- Google Sheets Config ---
GOOGLE_CREDENTIALS_JSON_CONTENT = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID', '1RTJmYTGMN-nH6X5eBdxfkIXjoBv1TD7Wy4w5-tW8VF8')
SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

# --- PostgreSQL Config ---
DATABASE_URL = os.getenv('DATABASE_URL')

# --- LeetCode Config ---
LEETCODE_USER = os.getenv('LEETCODE_USER')

# --- Application Logic Config ---
INITIAL_CUTOFF_DATETIME_IST_STR = "2025-05-17 08:00:00" # Your original cutoff
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')
UTC = pytz.utc
try:
    naive_dt = datetime.strptime(INITIAL_CUTOFF_DATETIME_IST_STR, "%Y-%m-%d %H:%M:%S")
    aware_dt_ist = IST_TIMEZONE.localize(naive_dt)
    INITIAL_CUTOFF_DATETIME_UTC = aware_dt_ist.astimezone(UTC)
except Exception as e:
    print(f"Error parsing INITIAL_CUTOFF_DATETIME_IST_STR: {e}")
    INITIAL_CUTOFF_DATETIME_UTC = datetime.now(UTC)

STATE_TABLE_NAME = "leetcode_sync_state"
STATE_KEY = "last_processed_leetcode_timestamp_utc"

COL_TOPIC = "Topic"
COL_PROBLEM = "Problem"
COL_CONFIDENCE = "confidence"
COL_LAST_VISITED = "Last Visited"
EXPECTED_HEADERS = [COL_TOPIC, COL_PROBLEM, COL_CONFIDENCE, COL_LAST_VISITED]

# --- Initialize Flask App ---
app = Flask(__name__)

# --- Global variable to track job status ---
sync_job_running = False

# --- DATABASE HELPER FUNCTIONS (Your existing functions - keep them as they are) ---
def get_db_connection():
    # ... (your existing code) ...
    """Establishes a connection to the PostgreSQL database."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def initialize_database():
    # ... (your existing code) ...
    """Creates the state table if it doesn't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    key VARCHAR(255) PRIMARY KEY,
                    value TIMESTAMPTZ
                );
            """).format(sql.Identifier(STATE_TABLE_NAME)))
        conn.commit()
        print(f"Database table '{STATE_TABLE_NAME}' initialized/checked.")
    except Exception as e:
        print(f"Error initializing database table: {e}")
    finally:
        if conn:
            conn.close()

def get_last_processed_timestamp():
    # ... (your existing code) ...
    """Retrieves the last processed timestamp from the database."""
    conn = get_db_connection()
    last_timestamp = None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql.SQL("SELECT value FROM {} WHERE key = %s")
                        .format(sql.Identifier(STATE_TABLE_NAME)), (STATE_KEY,))
            result = cur.fetchone()
            if result and result['value']:
                last_timestamp = result['value'].replace(tzinfo=pytz.utc) # Ensure it's UTC aware
    except psycopg2.Error as e: # Catch only psycopg2 errors here
        print(f"Database error retrieving last processed timestamp: {e}")
    except Exception as e:
        print(f"Generic error retrieving last processed timestamp: {e}")
    finally:
        if conn:
            conn.close()

    if last_timestamp:
        print(f"Retrieved last processed timestamp: {last_timestamp}")
        return last_timestamp
    else:
        print(f"No last processed timestamp found in DB, using initial cutoff: {INITIAL_CUTOFF_DATETIME_UTC}")
        return INITIAL_CUTOFF_DATETIME_UTC


def update_last_processed_timestamp(new_timestamp_utc):
    # ... (your existing code) ...
    """Updates the last processed timestamp in the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                INSERT INTO {} (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
            """).format(sql.Identifier(STATE_TABLE_NAME)), (STATE_KEY, new_timestamp_utc))
        conn.commit()
        print(f"Updated last processed timestamp to: {new_timestamp_utc}")
    except Exception as e:
        print(f"Error updating last processed timestamp: {e}")
    finally:
        if conn:
            conn.close()
# --- GOOGLE SHEETS HELPER FUNCTIONS (Your existing functions - keep them as they are) ---
def get_gspread_client():
    # ... (your existing code from the previous version, handling GOOGLE_CREDENTIALS_JSON_CONTENT and fallback) ...
    """Authenticates and returns a gspread client."""
    # print("Authenticating with Google Sheets...") # Optional debug
    # print(f"DEBUG: GOOGLE_CREDENTIALS_JSON_CONTENT is {'set' if GOOGLE_CREDENTIALS_JSON_CONTENT else 'not set'}")

    if GOOGLE_CREDENTIALS_JSON_CONTENT:
        try:
            creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON_CONTENT)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
            client = gspread.authorize(creds)
            return client
        except Exception as e:
            print(f"Error authenticating with Google Sheets using JSON content: {e}")
            raise
    else: 
        try:
            # print("Using credentials.json file for authentication.") # Optional debug
            if not os.path.exists('credentials.json'):
                print("CRITICAL: credentials.json file not found in the current directory.")
                raise FileNotFoundError("credentials.json file not found.")
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            client = gspread.authorize(creds)
            return client
        except Exception as e:
            print(f"Failed to authenticate with credentials.json. Error: {e}")
            raise

def get_sheet_data(client):
    # ... (your existing code from the previous version, including hyperlink parsing fix) ...
    """Fetches all data from the specified Google Sheet."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
        print(f"Accessing sheet: {sheet.title}")
        
        all_values = sheet.get_all_values()
        if not all_values:
            return {}, sheet, 1 
            
        header_row = all_values[0]
        for h_expected in EXPECTED_HEADERS:
            if h_expected not in header_row:
                actual_headers_str = ", ".join(f"'{h}'" for h in header_row)
                print(f"Error: Expected header '{h_expected}' not found in sheet headers. Found: [{actual_headers_str}]")
                raise Exception(f"Sheet header misconfiguration. Expected '{h_expected}'. Please check column names and case in your Google Sheet.")
        
        problem_col_index_0based = header_row.index(COL_PROBLEM) 
        last_visited_col_index_0based = header_row.index(COL_LAST_VISITED)

        sheet_problems = {} 
        for row_num, row_data in enumerate(all_values[1:], start=2): 
            if len(row_data) > problem_col_index_0based and row_data[problem_col_index_0based]: 
                problem_name_in_sheet = row_data[problem_col_index_0based]
                
                problem_name = problem_name_in_sheet
                if problem_name_in_sheet.startswith('=HYPERLINK("'):
                    try:
                        parts = problem_name_in_sheet.split('"')
                        if len(parts) >= 4:
                           problem_name = parts[3] 
                    except Exception:
                        pass # Keep original if parsing fails

                last_visited_str = ""
                if len(row_data) > last_visited_col_index_0based:
                    last_visited_str = row_data[last_visited_col_index_0based]

                sheet_problems[problem_name] = {
                    'row_number': row_num,
                    'last_visited': last_visited_str 
                }
        return sheet_problems, sheet, len(all_values) + 1
    except Exception as e:
        print(f"Error getting sheet data: {e}")
        raise

# --- LEETCODE DATA FETCHING (Your existing functions - keep them as they are) ---
def get_topic_tags_for_problem_public(title_slug):
    # ... (your existing code) ...
    """
    Fetches topic tags for a problem.
    """
    graphql_endpoint = "https://leetcode.com/graphql/"
    query = """
    query questionData($titleSlug: String!) {
      question(titleSlug: $titleSlug) {
        topicTags {
          name
          slug
        }
      }
    }
    """
    variables = {"titleSlug": title_slug}
    payload = {"query": query, "variables": variables}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "LeetCodeSheetSync/1.0 (Topic Tag Fetcher)"
    }
    try:
        time.sleep(0.5) 
        response = requests.post(graphql_endpoint, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if 'data' in data and data['data'] and data['data']['question'] and data['data']['question']['topicTags']:
            return [tag['name'] for tag in data['data']['question']['topicTags']]
    except Exception as e:
        print(f"Warning: Could not fetch topic tags for {title_slug}: {e}")
    return ["Uncategorized"]


def fetch_leetcode_submissions(last_processed_ts_utc):
    # ... (your existing code) ...
    """
    Fetches recent publicly available AC submissions for a user via LeetCode's GraphQL API.
    """
    if not LEETCODE_USER:
        print("ERROR: LEETCODE_USER environment variable is not set.")
        raise ValueError("LEETCODE_USER is required for fetching public submissions.")

    leetcode_username = LEETCODE_USER
    graphql_endpoint = "https://leetcode.com/graphql/"
    query = """
    query recentAcSubmissions($username: String!, $limit: Int!) {
      recentAcSubmissionList(username: $username, limit: $limit) {
        id
        title
        titleSlug
        timestamp
      }
    }
    """
    variables = {"username": leetcode_username, "limit": 20 }
    payload = {"query": query, "variables": variables}
    headers = {
        "Content-Type": "application/json",
        "Referer": f"https://leetcode.com/u/{leetcode_username}/",
        "User-Agent": "LeetCodeSheetSync/1.0 (Public API)"
    }
    print(f"Fetching recent public submissions for user: {leetcode_username}")
    try:
        response = requests.post(graphql_endpoint, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching LeetCode submissions via public API: {e}")
        return []

    submissions = []
    if 'data' in data and data['data'] and 'recentAcSubmissionList' in data['data'] and data['data']['recentAcSubmissionList'] is not None:
        for sub in data['data']['recentAcSubmissionList']:
            submission_timestamp_unix = int(sub['timestamp'])
            submission_dt_utc = datetime.fromtimestamp(submission_timestamp_unix, tz=UTC)
            if submission_dt_utc <= last_processed_ts_utc:
                continue
            
            topic_tags = get_topic_tags_for_problem_public(sub['titleSlug'])
            submissions.append({
                "name": sub['title'],
                "url": f"https://leetcode.com/problems/{sub['titleSlug']}/",
                "timestamp_utc": submission_dt_utc,
                "topic_tags": topic_tags
            })
        print(f"Fetched {len(data['data']['recentAcSubmissionList'])} submissions from public API, {len(submissions)} are new and after {last_processed_ts_utc}.")
    else:
        print(f"No 'recentAcSubmissionList' data found in public API response or data is None. Response: {data}")
    return submissions

# --- CORE SYNCHRONIZATION LOGIC (was previously main()) ---
def run_synchronization_logic():
    # ... (your existing code from the previous version, ensure `sync_job_running = False` is in a finally block if errors can occur) ...
    global sync_job_running 
    sync_job_running = True # Set flag at the beginning
    
    try:
        print(f"Synchronization logic started at {datetime.now(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
        # ... (rest of your sync logic, identical to the previous version) ...
        if not LEETCODE_USER:
            print("CRITICAL: LEETCODE_USER environment variable is not set. Cannot proceed.")
            return # Exit this function

        # 1. Initialize DB
        last_processed_ts_utc = INITIAL_CUTOFF_DATETIME_UTC 
        if DATABASE_URL:
            try:
                initialize_database() 
                last_processed_ts_utc = get_last_processed_timestamp()
            except Exception as e:
                print(f"Error with database initialization or getting last timestamp: {e}. Defaulting to initial cutoff.")
        else:
            print("WARNING: DATABASE_URL not set. State will not be persisted. Using initial cutoff as last processed time.")
        
        print(f"Will process LeetCode submissions after: {last_processed_ts_utc}")

        # 2. Fetch LeetCode Submissions
        try:
            leetcode_submissions = fetch_leetcode_submissions(last_processed_ts_utc)
        except Exception as e:
            print(f"CRITICAL: Failed to fetch LeetCode submissions: {e}")
            return

        if not leetcode_submissions:
            print("No new LeetCode submissions to process since last run.")
            return
        
        leetcode_submissions.sort(key=lambda s: s['timestamp_utc'])
        new_max_processed_timestamp_this_run = last_processed_ts_utc

        # 3. Get Google Sheet Data
        gs_client = None
        try:
            gs_client = get_gspread_client()
            sheet_problems_map, sheet_obj, _ = get_sheet_data(gs_client)
        except Exception as e:
            print(f"CRITICAL: Failed to get Google Sheet data or client: {e}")
            return

        updates_to_sheet_cells = [] 
        new_rows_to_add_data = []  

        for submission in leetcode_submissions:
            problem_name_from_leetcode = submission['name']
            problem_url = submission['url']
            submission_dt_utc = submission['timestamp_utc']

            if submission_dt_utc <= INITIAL_CUTOFF_DATETIME_UTC: 
                continue
            
            submission_date_for_sheet = submission_dt_utc.strftime('%m/%d/%Y')
            problem_hyperlink = f'=HYPERLINK("{problem_url}","{problem_name_from_leetcode}")'
            topic = ", ".join(submission.get('topic_tags', ["Uncategorized"]))

            matched_problem_in_sheet = sheet_problems_map.get(problem_name_from_leetcode)
            
            if matched_problem_in_sheet:
                existing_entry = matched_problem_in_sheet
                try:
                    # Ensure existing_entry['last_visited'] is not empty before parsing
                    current_sheet_date_obj = date.min 
                    if existing_entry['last_visited']:
                         current_sheet_date_obj = datetime.strptime(existing_entry['last_visited'], '%m/%d/%Y').date()
                except ValueError:
                    current_sheet_date_obj = date.min 

                if submission_dt_utc.date() > current_sheet_date_obj:
                    row_to_update = existing_entry['row_number']
                    try:
                        # Find the 'Last Visited' column index (0-based for list, then +1 for gspread Cell)
                        header_list_for_index = [COL_TOPIC, COL_PROBLEM, COL_CONFIDENCE, COL_LAST_VISITED] # ensure this matches what get_sheet_data uses to build the map
                        last_visited_col_idx_1based = header_list_for_index.index(COL_LAST_VISITED) + 1
                        updates_to_sheet_cells.append(gspread.Cell(row_to_update, last_visited_col_idx_1based, submission_date_for_sheet))
                        print(f"Marking for update '{problem_name_from_leetcode}' (Row {row_to_update}): Last Visited to {submission_date_for_sheet}")
                    except ValueError:
                        print(f"ERROR: Could not find '{COL_LAST_VISITED}' in EXPECTED_HEADERS for updating. Check config.")
            else:
                new_row_data = [topic, problem_hyperlink, "", submission_date_for_sheet]
                new_rows_to_add_data.append(new_row_data)
                print(f"Marking for add: '{problem_name_from_leetcode}' with Last Visited {submission_date_for_sheet}")
            
            if submission_dt_utc > new_max_processed_timestamp_this_run:
                new_max_processed_timestamp_this_run = submission_dt_utc
        
        if updates_to_sheet_cells:
            try:
                print(f"Applying {len(updates_to_sheet_cells)} cell updates to the sheet.")
                sheet_obj.update_cells(updates_to_sheet_cells, value_input_option='USER_ENTERED')
                print("Sheet cell updates applied.")
            except Exception as e:
                print(f"Error batch updating sheet cells: {e}")

        if new_rows_to_add_data:
            try:
                print(f"Adding {len(new_rows_to_add_data)} new rows to the sheet.")
                sheet_obj.append_rows(new_rows_to_add_data, value_input_option='USER_ENTERED')
                print("New rows added to sheet.")
            except Exception as e:
                print(f"Error batch adding new rows: {e}")
                
        if DATABASE_URL and new_max_processed_timestamp_this_run > last_processed_ts_utc:
            update_last_processed_timestamp(new_max_processed_timestamp_this_run)
        elif not DATABASE_URL:
            print("DATABASE_URL not set, so not updating last processed timestamp.")
        elif new_max_processed_timestamp_this_run == last_processed_ts_utc and leetcode_submissions:
             print("Processed submissions, but the latest submission timestamp matches the last processed. State timestamp remains unchanged.")
        else: 
            print("No new problem timestamps processed beyond the last recorded one or no submissions processed. State timestamp remains unchanged.")

        print(f"Synchronization logic finished at {datetime.now(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

    finally:
        sync_job_running = False # Ensure flag is reset even if errors occur


# --- FLASK API ENDPOINT (Simplified) ---
@app.route('/trigger-sync', methods=['GET']) # Changed to GET, removed POST
def handle_trigger_sync():
    global sync_job_running
    
    # WARNING: This endpoint is now unauthenticated. Anyone can trigger it.
    print("Public API trigger received for /trigger-sync (GET).")

    if sync_job_running:
        print("Sync job trigger requested, but a job is already running.")
        return jsonify({"status": "warning", "message": "Sync job already in progress. Please try again later."}), 409 # 409 Conflict

    print("Starting synchronization logic in a background thread.")
    thread = threading.Thread(target=run_synchronization_logic)
    thread.start()
    
    return jsonify({"status": "success", "message": "Synchronization job triggered successfully. Check server logs for progress."}), 202

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat(), "job_running": sync_job_running}), 200


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080)) 
    print(f"Flask app starting on port {port}")
    print("WARNING: The /trigger-sync endpoint is PUBLIC and UNSECURED.")
    print("Ensure this is acceptable for your use case.")
    # For Render, gunicorn will run the app. This `app.run` is for local development.
    app.run(host="0.0.0.0", port=5000, debug=True)
