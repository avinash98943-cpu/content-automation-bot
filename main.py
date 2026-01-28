import os
import time
import requests
import json
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google import genai
from google.genai import types

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"] 

# Initialize the NEW Google GenAI Client
client = genai.Client(api_key=GEMINI_API_KEY)

# --- GOOGLE SHEETS SETUP ---
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = service_account.Credentials.from_service_account_info(
    creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
service = build('sheets', 'v4', credentials=creds)

def send_slack_msg(blocks):
    """Helper to send valid Slack messages."""
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
    except Exception as e:
        print(f"Slack Error: {e}")

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
                # Default to 600s if duration missing, to ensure we process it
                duration = float(row[2]) if len(row) > 2 else 600
                if duration > 300:
                    pending.append({"row": i + 1, "url": row[1]})
            except:
                pass 
    return pending

def clean_json_text(text):
    """Cleans AI response to ensure valid JSON."""
    text = text.replace("```json", "").replace("```", "").strip()
    # Sometimes AI adds a preamble, find the first '{' and last '}'
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end != -1:
        return text[start:end]
    return text

def main():
    print("--- Starting Bot (New GenAI SDK) ---")
    
    # 0. Test Slack Connection First
    print("Testing Slack connection...")
    send_slack_msg([{"type": "section", "text": {"type": "mrkdwn", "text": "🤖 *Bot Started Processing...*"}}])

    target_posts = get_settings()
    calls = get_pending_calls()
    print(f"Found {len(calls)} pending calls.")

    processed_count = 0
    
    for call in calls:
        if processed_count >= target_posts:
            break
            
        print(f"Processing Row {call['row']}...")
        tmp_path = None

        try:
            # 1. Download Audio
            resp = requests.get(call['url'])
            if resp.status_code != 200:
                print("Failed to download audio.")
                continue

            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
                tmp.write(resp.content)
                tmp_path = tmp.name

            # 2. Upload using New SDK (FIXED: uses file= instead of path=)
            print("Uploading to Gemini...")
            file_ref = client.files.upload(file=tmp_path)
            
            # Wait for processing
            while file_ref.state.name == "PROCESSING":
                print("Waiting for audio processing...")
                time.sleep(2)
                file_ref = client.files.get(name=file_ref.name)
            
            if file_ref.state.name == "FAILED":
                print("Audio processing failed.")
                continue

            # 3. Analyze Call
            print("Analyzing...")
            prompt_analysis = """
            I am an Insurance Analyst. Listen to this Tamil sales call.
            Task 1: Transcribe the key conversation points (Summary Transcript) in English.
            Task 2: Identify the specific customer pain point.
            Task 3: Score this call (0-10) on 'Viral Marketing Potential'.
            
            Return JSON ONLY: {"transcript_summary": "...", "pain_point": "...", "score": 8}
            """
            
            # Use Flash for analysis
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=[file_ref, prompt_analysis]
            )
            
            analysis = json.loads(clean_json_text(response.text))

            # 4. Save to Sheet
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"Calls!D{call['row']}:F{call['row']}",
                valueInputOption="RAW",
                body={"values": [["Processed", analysis.get('score', 0), analysis.get('transcript_summary', '')]]}
            ).execute()

            # 5. Generate Post (If Score >= 6)
            if analysis.get('score', 0) >= 6:
                print("Generating Content...")
                prompt_post = f"""
                Context: {analysis['pain_point']}
                Transcript: {analysis['transcript_summary']}
                
                Create a Social Media Carousel (3 Slides) in ENGLISH and TAMIL.
                Also generate 3 different catchy Hooks.
                
                Return JSON ONLY: 
                {{
                    "hooks": ["Hook1", "Hook2", "Hook3"], 
                    "english_slides": ["Slide1", "Slide2", "Slide3"], 
                    "tamil_slides": ["Slide1", "Slide2", "Slide3"]
                }}
                """
                
                post_resp = client.models.generate_content(
                    model="gemini-1.5-flash",
                    contents=[prompt_post]
                )
                content = json.loads(clean_json_text(post_resp.text))
                
                if content:
                    # Send to Slack
                    blocks = [
                        {"type": "header", "text": {"type": "plain_text", "text": "🚀 Viral Content Generated"}},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Issue:* {analysis['pain_point']}\n*Score:* {analysis['score']}"}},
                        {"type": "divider"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*🎣 Hooks:*\n• " + "\n• ".join(content.get('hooks', [])) }},
                        {"type": "divider"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*🇬🇧 English Slides:*\n" + "\n".join(content.get('english_slides', [])) }},
                        {"type": "divider"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"*🇮🇳 Tamil Slides:*\n" + "\n".join(content.get('tamil_slides', [])) }}
                    ]
                    send_slack_msg(blocks)
                    print("Sent to Slack!")
                    processed_count += 1
            
        except Exception as e:
            print(f"Error processing row {call['row']}: {e}")
            send_slack_msg([{"type": "section", "text": {"type": "mrkdwn", "text": f"⚠️ Error processing Row {call['row']}: {str(e)}"}}])
            
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

if __name__ == "__main__":
    main()
