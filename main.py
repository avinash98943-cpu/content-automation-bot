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
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Settings!B1:B2"
        ).execute()
        rows = result.get('values', [])
        num_posts = int(rows[0][0]) if rows else 2
        return num_posts
    except Exception as e:
        print(f"Error reading settings (using default 2): {e}")
        return 2

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
                # If duration is missing or just a check, assume valid
                duration = float(row[2]) if len(row) > 2 else 600
                if duration > 300:
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
        print("Uploading to Gemini...")
        myfile = genai.upload_file(tmp_path)
        
        # Wait for file processing (important for large files)
        while myfile.state.name == "PROCESSING":
            print("Processing audio...")
            time.sleep(2)
            myfile = genai.get_file(myfile.name)

        # 3. TRANSCRIPTION & SCORING (Updated Model Name)
        print("Analyzing with Gemini Flash...")
        model = genai.GenerativeModel("gemini-1.5-flash-001")
        
        prompt_analysis = """
        I am an Insurance Analyst. Listen to this Tamil sales call.
        
        Task 1: Transcribe the key conversation points (Summary Transcript) in English.
        Task 2: Identify the specific customer pain point or confusion.
        Task 3: Score this call (0-10) on 'Viral Marketing Potential'. 
                (High score = unique objection, emotional story, or massive misconception).
        
        Return JSON ONLY:
        {
            "transcript_summary": "...",
            "pain_point": "...",
            "score": 8,
            "reason_for_score": "..."
        }
        """
        res = model.generate_content([myfile, prompt_analysis])
        
        # Clean response
        text = res.text.replace("```json", "").replace("```", "").strip()
        analysis = json.loads(text)
        
        return analysis

    except Exception as e:
        print(f"Error during analysis: {e}")
        return {"score": 0, "transcript_summary": "Error", "pain_point": "Error"}
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

def generate_viral_posts(transcript, pain_point):
    # Updated Model Name
    model = genai.GenerativeModel("gemini-1.5-pro-001")
    
    prompt_content = f"""
    Context: A customer had this issue: "{pain_point}".
    Transcript Summary: "{transcript}"
    
    Create a Social Media Carousel (3 Slides) in ENGLISH and TAMIL.
    Also generate 3 different catchy Hooks.
    
    Output Format (JSON ONLY):
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
    try:
        res = model.generate_content(prompt_content)
        text = res.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"Error generating post: {e}")
        return None

def main():
    print("Starting Bot...")
    
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
        print(f"Saving transcript for Row {call['row']}...")
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
    analyzed_calls.sort(key=lambda x: x.get('score', 0), reverse=True)
    winners = analyzed_calls[:target_posts]

    # 5. Generate Final Content for Winners
    for winner in winners:
        if winner.get('score', 0) > 0:
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
                print("Sent to Slack!")

if __name__ == "__main__":
    main()
