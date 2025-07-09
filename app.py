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
from bs4 import BeautifulSoup
import re

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

# --- GeeksforGeeks Config ---
GFG_USER = os.getenv('GFG_USER')

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

# Use a more generic state key
STATE_TABLE_NAME = "sync_state"
STATE_KEY = "last_processed_timestamp_utc"

# Add a 'Platform' column
COL_PLATFORM = "Platform"
COL_TOPIC = "Topic"
COL_PROBLEM = "Problem"
COL_CONFIDENCE = "confidence"
COL_LAST_VISITED = "Last Visited"
EXPECTED_HEADERS = [COL_PLATFORM, COL_TOPIC, COL_PROBLEM, COL_CONFIDENCE, COL_LAST_VISITED]

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

def fetch_gfg_submissions(last_processed_ts_utc):
    """
    Fetches recent solved problems for a user by simulating the API call
    made by the GFG profile page.
    """
    if not GFG_USER:
        print("WARNING: GFG_USER environment variable is not set. Skipping GFG sync.")
        return []

    gfg_username = GFG_USER
    # This is the specific endpoint the profile page uses to load solved problems
    url = f"https://www.geeksforgeeks.org/user-profile/problem-solved/{gfg_username}/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        # This header makes the request look like an AJAX call, which is what the browser does
        "X-Requested-With": "XMLHttpRequest"
    }
    
    # The request requires the username to be sent in the POST request body
    payload = {'user': gfg_username}

    print(f"Fetching GFG solved problems for user: {gfg_username}")
    try:
        # We need to use a POST request to this endpoint
        response = requests.post(url, headers=headers, data=payload, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching GFG profile data: {e}")
        return []

    # The response is HTML, so we parse it with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    submissions = []
    
    # The problems are contained within <a> tags inside a specific div structure
    # Example structure: <a href="/problems/problem-name/0" class="problem-solved-gfg">Problem Name</a>
    # The date is a sibling: <span class="problem-solved-date">15 May, 2025</span>
    problem_links = soup.find_all('a', class_='problem-solved-gfg')
    
    if not problem_links:
        print("Could not find any solved problems in the response from GFG. The page structure may have changed again.")
        return []

    for link_tag in problem_links:
        problem_name = link_tag.text.strip()
        problem_url = "https://www.geeksforgeeks.org" + link_tag['href']
        
        # The date is in a sibling span
        date_tag = link_tag.find_next_sibling('span', class_='problem-solved-date')
        if not date_tag:
            continue
            
        date_str = date_tag.text.strip()
        
        try:
            # --- IMPORTANT TIMESTAMP NOTE ---
            # GFG only provides the date (e.g., "06 Jun, 2025"), not the time.
            # We will parse the date and set the time to midnight UTC for consistency.
            submission_dt_naive = datetime.strptime(date_str, "%d %b, %Y")
            submission_dt_utc = UTC.localize(submission_dt_naive)

            if submission_dt_utc > last_processed_ts_utc:
                submissions.append({
                    "name": problem_name,
                    "url": problem_url,
                    "timestamp_utc": submission_dt_utc,
                    "topic_tags": ["Practice"], # GFG API doesn't provide specific topics here
                    "platform": "GFG"
                })
        except ValueError:
            print(f"Warning: Could not parse date '{date_str}' for problem '{problem_name}'. Skipping.")
            continue

    print(f"Fetched {len(problem_links)} GFG solved problems from API, {len(submissions)} are new.")
    return submissions

# --- CORE SYNCHRONIZATION LOGIC (was previously main()) ---
def run_synchronization_logic():
    global sync_job_running
    sync_job_running = True

    try:
        print(f"Synchronization logic started at {datetime.now(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

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

        print(f"Will process submissions after: {last_processed_ts_utc}")

        # 2. Fetch Submissions from All Platforms
        all_new_submissions = []
        if LEETCODE_USER:
            try:
                leetcode_submissions = fetch_leetcode_submissions(last_processed_ts_utc)
                # Add platform to each submission
                for sub in leetcode_submissions:
                    sub['platform'] = 'LeetCode'
                all_new_submissions.extend(leetcode_submissions)
            except Exception as e:
                print(f"CRITICAL: Failed to fetch LeetCode submissions: {e}")
        else:
            print("LEETCODE_USER not set, skipping LeetCode.")


        if GFG_USER:
            try:
                gfg_submissions = fetch_gfg_submissions(last_processed_ts_utc)
                all_new_submissions.extend(gfg_submissions)
            except Exception as e:
                print(f"CRITICAL: Failed to fetch GFG submissions: {e}")
        else:
            print("GFG_USER not set, skipping GFG.")

        if not all_new_submissions:
            print("No new submissions to process from any platform.")
            return

        all_new_submissions.sort(key=lambda s: s['timestamp_utc'])
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

        for submission in all_new_submissions:
            problem_name_from_platform = submission['name']
            problem_url = submission['url']
            submission_dt_utc = submission['timestamp_utc']

            if submission_dt_utc <= INITIAL_CUTOFF_DATETIME_UTC:
                continue

            # NEW: Format timestamp with time
            submission_datetime_for_sheet = submission_dt_utc.astimezone(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')

            problem_hyperlink = f'=HYPERLINK("{problem_url}","{problem_name_from_platform}")'
            topic = ", ".join(submission.get('topic_tags', ["Uncategorized"]))
            platform = submission.get('platform', 'Unknown')

            matched_problem_in_sheet = sheet_problems_map.get(problem_name_from_platform)

            if matched_problem_in_sheet:
                # Update logic remains largely the same, but we update the timestamp
                row_to_update = matched_problem_in_sheet['row_number']
                try:
                    header_list_for_index = EXPECTED_HEADERS
                    last_visited_col_idx_1based = header_list_for_index.index(COL_LAST_VISITED) + 1
                    platform_col_idx_1based = header_list_for_index.index(COL_PLATFORM) + 1

                    updates_to_sheet_cells.append(gspread.Cell(row_to_update, last_visited_col_idx_1based, submission_datetime_for_sheet))
                    updates_to_sheet_cells.append(gspread.Cell(row_to_update, platform_col_idx_1based, platform)) # Also update the platform
                    print(f"Marking for update '{problem_name_from_platform}' (Row {row_to_update}): Last Visited to {submission_datetime_for_sheet}")
                except ValueError as e:
                    print(f"ERROR: Could not find a column in EXPECTED_HEADERS for updating: {e}")

            else:
                # NEW: Add platform to the new row
                new_row_data = [platform, topic, problem_hyperlink, "", submission_datetime_for_sheet]
                new_rows_to_add_data.append(new_row_data)
                print(f"Marking for add: '{problem_name_from_platform}' from {platform} with Last Visited {submission_datetime_for_sheet}")

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
        else:
            print("No new problem timestamps processed beyond the last recorded one. State timestamp remains unchanged.")

        print(f"Synchronization logic finished at {datetime.now(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

    finally:
        sync_job_running = False

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
    app.run(host='0.0.0.0', port=port)