import os
import time
import requests
import json
import tempfile
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CONFIGURATION ---
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
    """Reads the 'Settings' tab safely."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Settings!B1:B2"
        ).execute()
        rows = result.get('values', [])
        return int(rows[0][0]) if rows else 2
    except:
        return 2

def get_pending_calls():
    """Gets calls from Sheet that are 'Pending' and > 5 mins."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="Calls!A:D"
    ).execute()
    rows = result.get('values', [])
    pending = []
    
    for i, row in enumerate(rows):
        # We need at least 4 columns (Status is index 3)
        if len(row) > 3 and row[3] == "Pending":
            try:
                # If duration is missing, default to 600 so we process it
                duration = float(row[2]) if len(row) > 2 else 600
                if duration > 300:
                    pending.append({"row": i + 1, "url": row[1]})
            except:
                pass 
    return pending

def robust_generate_content(prompt, file_attachment=None):
    """The Fail-Safe Generator: Tries multiple models until one works."""
    # List of models to try in order of preference
    models_to_try = [
        "gemini-1.5-flash",
        "gemini-1.5-flash-001",
        "gemini-1.5-pro", 
        "gemini-1.5-pro-001",
        "gemini-pro"
    ]

    last_error = None

    for model_name in models_to_try:
        try:
            print(f"🔄 Attempting with model: {model_name}...")
            model = genai.GenerativeModel(model_name)
            
            if file_attachment:
                response = model.generate_content([file_attachment, prompt])
            else:
                response = model.generate_content(prompt)
                
            print(f"✅ Success with {model_name}!")
            
            # Clean JSON formatting issues common in AI response
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)

        except Exception as e:
            print(f"❌ {model_name} failed. Reason: {e}")
            last_error = e
            time.sleep(1) # Brief pause before trying next model
            continue

    # If we get here, ALL models failed
    print("🚨 All models failed.")
    return None

def process_call_workflow(call):
    row_num = call['row']
    audio_url = call['url']
    
    # 1. Download Audio
    print(f"Downloading audio from Row {row_num}...")
    resp = requests.get(audio_url)
    if resp.status_code != 200:
        print("Failed to download audio.")
        return None

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
        tmp.write(resp.content)
        tmp_path = tmp.name

    try:
        # 2. Upload to Gemini
        print("Uploading file to Gemini...")
        myfile = genai.upload_file(tmp_path)
        
        # Wait for processing
        while myfile.state.name == "PROCESSING":
            print("Waiting for file processing...")
            time.sleep(2)
            myfile = genai.get_file(myfile.name)

        # 3. ANALYZE (Using Fail-Safe Function)
        print("Analyzing Call...")
        prompt_analysis = """
        I am an Insurance Analyst. Listen to this Tamil sales call.
        
        Task 1: Transcribe the key conversation points (Summary Transcript) in English.
        Task 2: Identify the specific customer pain point or confusion.
        Task 3: Score this call (0-10) on 'Viral Marketing Potential'.
        
        Return JSON ONLY:
        {
            "transcript_summary": "...",
            "pain_point": "...",
            "score": 8
        }
        """
        analysis_data = robust_generate_content(prompt_analysis, myfile)
        
        if not analysis_data:
            return None

        # 4. Save Transcript to Sheet
        print(f"Saving transcript to Sheet Row {row_num}...")
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"Calls!D{row_num}:F{row_num}",
            valueInputOption="RAW",
            body={"values": [["Processed", analysis_data.get('score', 0), analysis_data.get('transcript_summary', '')]]}
        ).execute()

        # 5. GENERATE POSTS (Only if score > 6)
        # We reuse the analysis data to create the post content
        post_content = None
        if analysis_data.get('score', 0) >= 6:
            print("High score detected! Generating viral posts...")
            prompt_content = f"""
            Context: Customer issue: "{analysis_data['pain_point']}".
            Transcript: "{analysis_data['transcript_summary']}"
            
            Create a Social Media Carousel (3 Slides) in ENGLISH and TAMIL.
            Also generate 3 different catchy Hooks.
            
            Return JSON ONLY:
            {{
                "hooks": ["Hook 1", "Hook 2", "Hook 3"],
                "english_slides": ["Slide 1", "Slide 2", "Slide 3"],
                "tamil_slides": ["Slide 1", "Slide 2", "Slide 3"]
            }}
            """
            post_content = robust_generate_content(prompt_content)
        
        return {
            "analysis": analysis_data,
            "posts": post_content
        }

    except Exception as e:
        print(f"Critical workflow error: {e}")
        return None
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

def main():
    print("--- Starting Daily Content Bot ---")
    
    # Check Settings
    target_posts = get_settings()
    print(f"Goal: {target_posts} posts.")

    # Get Pending Calls
    calls = get_pending_calls()
    print(f"Found {len(calls)} pending calls.")

    processed_count = 0
    
    for call in calls:
        if processed_count >= target_posts:
            break
            
        result = process_call_workflow(call)
        
        if result and result['posts']:
            # Send to Slack
            p = result['analysis']['pain_point']
            score = result['analysis']['score']
            hooks = "\n".join([f"• {h}" for h in result['posts']['hooks']])
            eng_slides = "\n".join([f"Slide {i+1}: {t}" for i,t in enumerate(result['posts']['english_slides'])])
            tam_slides = "\n".join([f"Slide {i+1}: {t}" for i,t in enumerate(result['posts']['tamil_slides'])])
            
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": "🚀 Viral Content Generated"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Issue:* {p}\n*Score:* {score}/10"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*🎣 Hooks:*\n{hooks}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*🇬🇧 English:*\n{eng_slides}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*🇮🇳 Tamil:*\n{tam_slides}"}}
            ]
            
            requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
            print("Sent to Slack!")
            processed_count += 1
            
    print("--- Bot Finished ---")

if __name__ == "__main__":
    main()
