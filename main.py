import os
import time
import requests
import json
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"] 

# --- HELPER: RAW GEMINI API (Bypasses Broken Library) ---
def gemini_upload_file(file_path, mime_type="audio/mp3"):
    """Uploads file using raw REST API to avoid library issues."""
    file_size = os.path.getsize(file_path)
    url = f"https://generativelanguage.googleapis.com/upload/v1beta/files?key={GEMINI_API_KEY}"
    
    # 1. Start Upload
    headers = {
        "X-Goog-Upload-Protocol": "resumable",
        "X-Goog-Upload-Command": "start",
        "X-Goog-Upload-Header-Content-Length": str(file_size),
        "X-Goog-Upload-Header-Content-Type": mime_type,
        "Content-Type": "application/json"
    }
    data = {"file": {"display_name": "audio_call"}}
    response = requests.post(url, headers=headers, json=data)
    upload_url = response.headers.get("X-Goog-Upload-URL")
    
    # 2. Upload Bytes
    with open(file_path, "rb") as f:
        headers = {
            "Content-Length": str(file_size),
            "X-Goog-Upload-Offset": "0",
            "X-Goog-Upload-Command": "upload, finalize"
        }
        response = requests.post(upload_url, headers=headers, data=f)
    
    file_info = response.json()
    file_uri = file_info['file']['uri']
    
    # 3. Wait for Processing
    print("Waiting for audio processing...")
    while True:
        check_url = f"https://generativelanguage.googleapis.com/v1beta/files/{file_info['file']['name']}?key={GEMINI_API_KEY}"
        state_resp = requests.get(check_url).json()
        state = state_resp.get("state")
        if state == "ACTIVE":
            break
        if state == "FAILED":
            raise Exception("File processing failed")
        time.sleep(2)
        
    return file_uri

def gemini_generate(prompt, file_uri=None):
    """Generates content using raw REST API."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    contents = []
    parts = []
    
    if file_uri:
        parts.append({"file_data": {"mime_type": "audio/mp3", "file_uri": file_uri}})
    
    parts.append({"text": prompt})
    contents.append({"parts": parts})
    
    payload = {"contents": contents}
    
    response = requests.post(url, json=payload)
    
    if response.status_code != 200:
        print(f"API Error: {response.text}")
        return None
        
    try:
        text = response.json()['candidates'][0]['content']['parts'][0]['text']
        # Clean JSON
        clean_text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_text)
    except Exception as e:
        print(f"Parsing Error: {e}")
        return None

# --- GOOGLE SHEETS SETUP ---
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = service_account.Credentials.from_service_account_info(
    creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
service = build('sheets', 'v4', credentials=creds)

def get_settings():
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Settings!B1:B2"
        ).execute()
        rows = result.get('values', [])
        return int(rows[0][0]) if rows else 2
    except:
        return 2

def get_pending_calls():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="Calls!A:D"
    ).execute()
    rows = result.get('values', [])
    pending = []
    for i, row in enumerate(rows):
        if len(row) > 3 and row[3] == "Pending":
            try:
                duration = float(row[2]) if len(row) > 2 else 600
                if duration > 300:
                    pending.append({"row": i + 1, "url": row[1]})
            except:
                pass 
    return pending

def main():
    print("--- Starting Bot (Raw Mode) ---")
    target_posts = get_settings()
    calls = get_pending_calls()
    print(f"Found {len(calls)} pending calls.")

    processed_count = 0
    
    for call in calls:
        if processed_count >= target_posts:
            break
            
        print(f"Processing Row {call['row']}...")
        
        # 1. Download Audio
        resp = requests.get(call['url'])
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        try:
            # 2. Upload (Raw API)
            file_uri = gemini_upload_file(tmp_path)
            
            # 3. Analyze
            prompt_analysis = """
            I am an Insurance Analyst. Listen to this Tamil sales call.
            Task 1: Transcribe the key conversation points (Summary Transcript) in English.
            Task 2: Identify the specific customer pain point.
            Task 3: Score this call (0-10) on 'Viral Marketing Potential'.
            Return JSON ONLY: {"transcript_summary": "...", "pain_point": "...", "score": 8}
            """
            analysis = gemini_generate(prompt_analysis, file_uri)
            
            if not analysis: continue

            # 4. Save to Sheet
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"Calls!D{call['row']}:F{call['row']}",
                valueInputOption="RAW",
                body={"values": [["Processed", analysis.get('score', 0), analysis.get('transcript_summary', '')]]}
            ).execute()

            # 5. Generate Post (if good score)
            if analysis.get('score', 0) >= 6:
                prompt_post = f"""
                Context: {analysis['pain_point']}
                Transcript: {analysis['transcript_summary']}
                Create a Social Media Carousel (3 Slides) in ENGLISH and TAMIL.
                Also generate 3 different catchy Hooks.
                Return JSON ONLY: {{"hooks": [], "english_slides": [], "tamil_slides": []}}
                """
                content = gemini_generate(prompt_post)
                
                if content:
                    # Send to Slack
                    blocks = [
                        {"type": "header", "text": {"type": "plain_text", "text": "🚀 Viral Content Generated"}},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Issue:* {analysis['pain_point']}\n*Score:* {analysis['score']}"}},
                        {"type": "divider"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*🇬🇧 English:*\n" + "\n".join(content.get('english_slides', [])) }},
                        {"type": "divider"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*🇮🇳 Tamil:*\n" + "\n".join(content.get('tamil_slides', [])) }}
                    ]
                    requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
                    print("Sent to Slack!")
                    processed_count += 1
                    
        except Exception as e:
            print(f"Error: {e}")
        finally:
            os.remove(tmp_path)

if __name__ == "__main__":
    main()
