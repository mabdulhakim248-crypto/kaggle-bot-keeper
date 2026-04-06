import os, json, base64, subprocess, sys, time, tempfile

BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
API_ID         = os.environ.get("API_ID", "38635106")
API_HASH       = os.environ.get("API_HASH", "e159cf0eb690131778801b19bfa8fb08")
YT_CLIENT_ID   = os.environ.get("YT_CLIENT_ID", "")
YT_CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET", "")
GH_SYNC_TOKEN  = os.environ.get("GH_SYNC_TOKEN", "")

def b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

radio_b64 = b64("radio.py")
yt_b64    = b64("youtube.py")
main_b64  = b64("main.py")

cell0 = f"""import subprocess, sys, os
os.environ['BOT_TOKEN']      = '{BOT_TOKEN}'
os.environ['API_ID']         = '{API_ID}'
os.environ['API_HASH']       = '{API_HASH}'
os.environ['SESSION_STRING'] = '{SESSION_STRING}'
os.environ['YT_CLIENT_ID']     = '{YT_CLIENT_ID}'
os.environ['YT_CLIENT_SECRET'] = '{YT_CLIENT_SECRET}'
print('Installing FFmpeg...')
subprocess.run(['apt-get','install','-y','-q','ffmpeg'], check=True)
print('Installing packages...')
subprocess.run([sys.executable,'-m','pip','install','-q','pyTelegramBotAPI','pyrogram','tgcrypto','yt-dlp','requests'], check=True)
print('Bot files will be written from embedded b64...')"""

cell1 = f"""import base64, json, urllib.request, os
RADIO_B64 = '{radio_b64}'
with open('radio.py','wb') as f:
    f.write(base64.b64decode(RADIO_B64))
print('radio.py updated!')
YT_B64 = '{yt_b64}'
with open('youtube.py','wb') as f:
    f.write(base64.b64decode(YT_B64))
print('youtube.py written!')
MAIN_B64 = '{main_b64}'
with open('main.py','wb') as f:
    f.write(base64.b64decode(MAIN_B64))
print('main.py written!')
GH_TOKEN = '{GH_SYNC_TOKEN}'
GH_REPO  = 'mabdulhakim248-crypto/kaggle-bot-keeper'
GH_PATH  = 'streams_state.json'
try:
    url = f'https://api.github.com/repos/{{GH_REPO}}/contents/{{GH_PATH}}'
    req = urllib.request.Request(url, headers={{
        'Authorization': f'token {{GH_TOKEN}}',
        'Accept': 'application/vnd.github.v3+json'}})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read())
        state = json.loads(base64.b64decode(d['content']).decode())
        with open('streams_state.json','w') as f: json.dump(state,f)
        tg = len(state.get('tg',{{}}))
        yt = len(state.get('yt',{{}}))
        print(f'State restored: {{tg}} TG + {{yt}} YT streams')
except Exception as e:
    print(f'No previous state ({{e}})')"""

cell2 = f"""import subprocess, os, sys
env = os.environ.copy()
env['BOT_TOKEN']      = '{BOT_TOKEN}'
env['API_ID']         = '{API_ID}'
env['API_HASH']       = '{API_HASH}'
env['SESSION_STRING'] = '{SESSION_STRING}'
env['YT_CLIENT_ID']     = '{YT_CLIENT_ID}'
env['YT_CLIENT_SECRET'] = '{YT_CLIENT_SECRET}'
env['GH_SYNC_TOKEN']  = '{GH_SYNC_TOKEN}'
print('Starting bot...')
subprocess.run([sys.executable,'main.py'],env=env)"""

nb = {{
    "metadata": {{"kernelspec": {{"display_name": "Python 3", "language": "python", "name": "python3"}}, "language_info": {{"name": "python"}}}},
    "nbformat": 4, "nbformat_minor": 4,
    "cells": [
        {{"cell_type":"code","execution_count":None,"metadata":{{}},"outputs":[],"source":[cell0]}},
        {{"cell_type":"code","execution_count":None,"metadata":{{}},"outputs":[],"source":[cell1]}},
        {{"cell_type":"code","execution_count":None,"metadata":{{}},"outputs":[],"source":[cell2]}},
    ]
}}

meta = {{
    "id": "jvnngmg/quran-radio-bot",
    "title": "Quran Radio Bot",
    "code_file": "nb.ipynb",
    "language": "python",
    "kernel_type": "notebook",
    "is_private": True,
    "enable_gpu": False,
    "enable_internet": True,
    "dataset_sources": [],
    "competition_sources": [],
    "kernel_sources": []
}}

d = tempfile.mkdtemp()
with open(f"{{d}}/nb.ipynb", "w") as f:
    json.dump(nb, f)
with open(f"{{d}}/kernel-metadata.json", "w") as f:
    json.dump(meta, f)

print("Pushing notebook to Kaggle...")
r = subprocess.run(["kaggle", "kernels", "push"], cwd=d, capture_output=True, text=True)
print(r.stdout, r.stderr)

print("Waiting for RUNNING...")
for i in range(20):
    time.sleep(30)
    s = subprocess.run(
        ["kaggle", "kernels", "status", "jvnngmg/quran-radio-bot"],
        capture_output=True, text=True
    ).stdout
    print(f"[{{i+1}}]", s.strip())
    if "RUNNING" in s.upper():
        print("Bot revived!")
        break
