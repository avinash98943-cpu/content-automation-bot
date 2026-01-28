import os
import time
import requests
import json
import tempfile
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import pytz

# --- SECRETS (Will be loaded from GitHub Env) ---
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
# We will write the credentials.json content into an ENV variable for GitHub
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"] 

genai.configure(api_key=GEMINI_API_KEY)

# --- GOOGLE SHEETS SETUP ---
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = service_account.Credentials.from_service_account_info(
    creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
service = build('sheets', 'v4', credentials=creds)

def get_settings():
    """Reads the 'Settings' tab."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="Settings!B1:B2"
    ).execute()
    rows = result.get('values', [])
    num_posts = int(rows[0][0]) if rows else 2
    # run_hour = int(rows[1][0]) if len(rows) > 1 else 20
    return num_posts

def get_pending_calls():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="Calls!A:D"
    ).execute()
    rows = result.get('values', [])
    pending = []
    # Skip header (start at index 1), check if col D (index 3) is 'Pending'
    for i, row in enumerate(rows):
        if len(row) > 3 and row[3] == "Pending":
            # Check duration > 300s (5 mins)
            try:
                if float(row[2]) > 300:
                    pending.append({"row": i + 1, "url": row[1]})
            except:
                pass 
    return pending

def process_call_with_gemini(audio_url):
    # 1. Download Audio
    print(f"Downloading {audio_url}...")
    resp = requests.get(audio_url)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
        tmp.write(resp.content)
        tmp_path = tmp.name

    try:
        # 2. Upload to Gemini
        myfile = genai.upload_file(tmp_path)
        
        # 3. TRANSCRIPTION & SCORING (Flash Model)
        model = genai.GenerativeModel("gemini-1.5-flash")
        
        prompt_analysis = """
        I am an Insurance Analyst. Listen to this Tamil sales call.
        
        Task 1: Transcribe the key conversation points (Summary Transcript) in English.
        Task 2: Identify the specific customer pain point or confusion.
        Task 3: Score this call (0-10) on 'Viral Marketing Potential'. 
                (High score = unique objection, emotional story, or massive misconception).
        
        Return JSON:
        {
            "transcript_summary": "...",
            "pain_point": "...",
            "score": 8,
            "reason_for_score": "..."
        }
        """
        res = model.generate_content([myfile, prompt_analysis])
        analysis = json.loads(res.text.replace("```json", "").replace("```", ""))
        
        # 4. CONTENT GENERATION (Pro Model - Better Writing)
        # Only run this if score is decent (>6) to save time, but we will run for top N later.
        # For now, just return analysis to rank them.
        return analysis

    except Exception as e:
        print(f"Error: {e}")
        return {"score": 0, "transcript_summary": "Error", "pain_point": "Error"}
    finally:
        os.remove(tmp_path)

def generate_viral_posts(transcript, pain_point):
    model = genai.GenerativeModel("gemini-1.5-pro")
    
    prompt_content = f"""
    Context: A customer had this issue: "{pain_point}".
    Transcript Summary: "{transcript}"
    
    Create a Social Media Carousel (3 Slides) in ENGLISH and TAMIL.
    Also generate 3 different catchy Hooks.
    
    Output Format (JSON):
    {{
        "hooks": ["Hook 1", "Hook 2", "Hook 3"],
        "english_slides": [
            "Slide 1 Text (The Problem)",
            "Slide 2 Text (The Story/Context)",
            "Slide 3 Text (The Solution/Lesson)"
        ],
        "tamil_slides": [
            "Slide 1 Text (Tamil Translation)",
            "Slide 2 Text (Tamil Translation)",
            "Slide 3 Text (Tamil Translation)"
        ]
    }}
    """
    res = model.generate_content(prompt_content)
    try:
        return json.loads(res.text.replace("```json", "").replace("```", ""))
    except:
        return None

def main():
    # 1. Read Settings
    target_posts = get_settings()
    print(f"Targeting {target_posts} posts for today.")

    # 2. Get Calls
    calls = get_pending_calls()
    print(f"Found {len(calls)} pending calls > 5 mins.")
    
    analyzed_calls = []

    # 3. Analyze All Pending Calls
    for call in calls:
        data = process_call_with_gemini(call['url'])
        
        # Save Transcript to Sheet immediately
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"Calls!D{call['row']}:F{call['row']}",
            valueInputOption="RAW",
            body={"values": [["Processed", data['score'], data['transcript_summary']]]}
        ).execute()
        
        call.update(data)
        analyzed_calls.append(call)
        time.sleep(2)

    # 4. Pick Winners
    analyzed_calls.sort(key=lambda x: x['score'], reverse=True)
    winners = analyzed_calls[:target_posts]

    # 5. Generate Final Content for Winners
    for winner in winners:
        print(f"Generating content for Top Call (Score: {winner['score']})")
        content = generate_viral_posts(winner['transcript_summary'], winner['pain_point'])
        
        if content:
            # Send to Slack
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": "🚀 Daily Viral Content Ready"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Source Issue:* {winner['pain_point']}\n*Score:* {winner['score']}/10"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": "*🎣 Choose a Hook:*\n1. " + "\n2. ".join(content['hooks']) }},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": "*🇬🇧 English Slides:*\n" + "\n---\n".join(content['english_slides']) }},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": "*🇮🇳 Tamil Slides:*\n" + "\n---\n".join(content['tamil_slides']) }},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*📝 Transcript Preview:*\n{winner['transcript_summary'][:200]}..."}}
            ]
            
            requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})

if __name__ == "__main__":
    main()
