"""
Zen Garden Monthly Wrapped — Automated Extractor
==================================================
Pulls a full month of #zengarden data and produces zen_garden_wrapped_data.json
which powers the index_wrapped.html page.

What it computes per person:
  - top_words, top_emojis, top_reactions, reactions_given
  - photos_shared, active_hour, active_label, bestie, most_hyped
  - pts, wk1, wk2, wk3, wk4 (read from zen_garden_weekly.json)
  - bonus tier ($50 / $25 / $15 / $0)
  - style (Pure Engager, Engager, Balanced, Broadcaster, Heavy Broadcaster)
  - trend (Up / Down)
  - streak (weeks active)
  - encourage (reactions given)
  - conn (thread connections)
  - dim (biggest wellness dimension, from local keyword classifier)
  - wellness, stress (post counts)
  - sentiment (lexicon-based)

Plus aggregates: total, active, participation_rate, total_pts, wk_totals,
dim_counts, avg_sentiment, positive_pct, stress_flags, styles, word_counts.
And awards: longest_streak, most_improved, most_loved, hype_machine, etc.

Usage:
  python zen_garden_wrapped_auto.py                   # auto-detect from wrapped_post_date
  python zen_garden_wrapped_auto.py --month "April 2026"
  python zen_garden_wrapped_auto.py --dry-run
"""

import os
import re
import sys
import json
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── PATHS ───────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "weekly_config.json")
WEEKLY_JSON = os.path.join(SCRIPT_DIR, "zen_garden_weekly.json")
WRAPPED_JSON = os.path.join(SCRIPT_DIR, "zen_garden_wrapped_data.json")

# ── SECRETS ─────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
USER_TOKEN = os.environ.get("SLACK_USER_TOKEN", "")

if not BOT_TOKEN or not USER_TOKEN:
    env_path = os.path.join(SCRIPT_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip()
        BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
        USER_TOKEN = os.environ.get("SLACK_USER_TOKEN", "")

    if not BOT_TOKEN or not USER_TOKEN:
        print("❌ Missing Slack tokens. Set SLACK_BOT_TOKEN and SLACK_USER_TOKEN.")
        sys.exit(1)


# ── STOPWORDS ─────────────────────────────────────────────────────
STOPWORDS = {
    'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'shall',
    'should', 'may', 'might', 'must', 'can', 'could', 'i', 'you', 'he',
    'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my',
    'your', 'his', 'its', 'our', 'their', 'this', 'that', 'these', 'those',
    'am', 'im', 'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from',
    'up', 'about', 'into', 'through', 'and', 'but', 'or', 'if', 'so', 'as',
    'than', 'just', 'because', 'while', 'when', 'where', 'why', 'how',
    'all', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such',
    'no', 'nor', 'not', 'only', 'own', 'same', 'too', 'very', 'lol', 'omg',
    'yeah', 'yes', 'ok', 'okay', 'haha', 'lmao', 'like', 'well', 'right',
    'oh', 'hey', 'one', 'two', 'really', 'much', 'still', 'even', 'way',
    'thing', 'things', 'know', 'think', 'want', 'see', 'look', 'make',
    'take', 'go', 'going', 'get', 'got', 'come', 'came', 'back', 'also',
    'amp', 'gt', 'lt', 'll', 've', 're', 's', 't', 'd', 'm',
    'de', 'en', 'la', 'el', 'y', 'que', 'es', 'un', 'una', 'por', 'con',
    'para', 'se', 'del', 'al', 'lo', 'los', 'las', 'como', 'pero', 'su',
    'le', 'ya', 'todo', 'mi', 'te', 'si', 'yo', 'tu', 'nos', 'fue', 'hay',
    'bien', 'tambien', 'muy', 'solo', 'hoy', 'dia', 'hace', 'mas', 'sin',
}


# ── DIMENSION KEYWORDS (local classifier — no API) ────────────────
DIMENSION_KEYWORDS = {
    'social': [
        'friend', 'family', 'team', 'community', 'together', 'hang', 'gather',
        'party', 'brunch', 'lunch', 'dinner', 'drinks', 'group', 'coworker',
        'colleague', 'meet', 'celebrate', 'birthday', 'wedding', 'date night',
        'love you', 'kids', 'son', 'daughter', 'mom', 'dad', 'husband', 'wife',
        'fianc', 'partner', 'baby', 'reunion', 'people'
    ],
    'physical': [
        'gym', 'workout', 'run ', 'running', 'yoga', 'pilates', 'hike', 'hiking',
        'walk', 'walking', 'swim', 'bike', 'cycl', 'stretch', 'lift', 'squat',
        'cardio', 'soccer', 'basketball', 'baseball', 'tennis', 'golf', 'sport',
        'fit', 'health', 'protein', 'salad', 'smoothie', 'meal', 'eat',
        'breakfast', 'cook', 'sleep', 'rest', 'energy', 'strong', 'muscle',
        'rowing', 'row', 'class', 'spin', 'lift', 'pickleball', 'dance'
    ],
    'emotional': [
        'feel', 'happy', 'sad', 'anxious', 'stress', 'calm', 'peace', 'mood',
        'mental', 'therapy', 'journal', 'meditate', 'breathe', 'gratitude',
        'grateful', 'thankful', 'joy', 'cry', 'smile', 'self-care', 'selfcare',
        'self care', 'healing', 'relax', 'overwhelm', 'tired', 'exhausted',
        'proud'
    ],
    'intellectual': [
        'book', 'read', 'learn', 'study', 'research', 'podcast', 'course',
        'class', 'knowledge', 'curious', 'discover', 'science', 'history',
        'art', 'music', 'write', 'paint', 'creative', 'project', 'puzzle',
        'documentary', 'lecture', 'language', 'recipe'
    ],
    'spiritual': [
        'meditate', 'mindful', 'pray', 'prayer', 'faith', 'soul', 'spirit',
        'church', 'temple', 'mass', 'reflect', 'purpose', 'meaning', 'sacred',
        'retreat', 'silence', 'devotion', 'belief', 'god', 'blessed'
    ],
    'occupational': [
        'work', 'job', 'career', 'profession', 'meeting', 'achievement',
        'promotion', 'certification', 'license', 'rbt', 'bcba', 'training',
        'workshop', 'conference', 'office', 'client', 'session', 'task',
        'goal', 'success', 'pass', 'passed', 'exam', 'interview', 'team',
        'amigo', 'aba'
    ],
    'environmental': [
        'nature', 'outdoor', 'park', 'beach', 'mountain', 'forest', 'ocean',
        'lake', 'river', 'garden', 'plant', 'flower', 'tree', 'bird', 'dog',
        'cat', 'pet', 'sky', 'sunset', 'sunrise', 'weather', 'sun', 'rain',
        'snow', 'camping', 'fresh air', 'view', 'environment', 'recycl',
        'puppy', 'kitten', 'animal'
    ],
    'financial': [
        'money', 'budget', 'save', 'saving', 'invest', 'stock', 'bond',
        'retirement', 'paycheck', 'salary', 'bonus', 'raise', 'expense',
        'debt', 'loan', 'mortgage', 'financial', 'finance', 'wealth',
        'tax', '401k', 'roth'
    ]
}


# ── SIMPLE SENTIMENT LEXICON ──────────────────────────────────────
POSITIVE_WORDS = {
    'love', 'amazing', 'great', 'awesome', 'beautiful', 'wonderful', 'happy',
    'fun', 'cute', 'good', 'nice', 'best', 'excellent', 'perfect', 'fantastic',
    'incredible', 'delicious', 'yummy', 'cozy', 'sweet', 'lovely', 'adorable',
    'fabulous', 'gorgeous', 'glad', 'excited', 'proud', 'thanks', 'thank',
    'grateful', 'blessed', 'enjoyed', 'enjoy', 'thrilled', 'celebrate', 'won',
    'success', 'congrats', 'hooray', 'yay'
}
NEGATIVE_WORDS = {
    'sad', 'tired', 'exhausted', 'stressed', 'stressful', 'awful', 'terrible',
    'bad', 'hate', 'horrible', 'sick', 'sore', 'hurt', 'pain', 'angry', 'mad',
    'frustrated', 'anxious', 'worried', 'worry', 'sorry', 'cry', 'crying',
    'overwhelmed', 'broken', 'lost', 'hard', 'difficult', 'tough', 'rough'
}


# ── EMOJI HELPERS ─────────────────────────────────────────────────
try:
    import emoji
    HAS_EMOJI_LIB = True
except ImportError:
    HAS_EMOJI_LIB = False

EMOJI_RE = re.compile(
    "[\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\U00002702-\U000027B0"
    "\U00002600-\U000026FF"
    "\U00002764"
    "]+", flags=re.UNICODE
)

def extract_emojis(text):
    if HAS_EMOJI_LIB:
        return [e['emoji'] for e in emoji.emoji_list(text)]
    return EMOJI_RE.findall(text)


def extract_words(text):
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'<[@#!][^>]+>', '', text)
    text = re.sub(r':[a-z0-9_+\-]+:', '', text)
    text = text.lower()
    words = re.findall(r'[a-záéíóúñüàèìòùâêîôû]{2,}', text)
    return [w for w in words if w not in STOPWORDS and len(w) >= 3]


def hour_label(h):
    if 5 <= h < 9: return "Early Bird 🐦"
    if 9 <= h < 12: return "Morning Person ☀️"
    if 12 <= h < 14: return "Lunch Poster 🍽️"
    if 14 <= h < 17: return "Afternoon Vibes 🌤️"
    if 17 <= h < 20: return "Evening Energy 🌆"
    if 20 <= h < 23: return "Night Owl 🦉"
    return "After Hours 🌙"


# ── DIMENSION CLASSIFIER (LOCAL, NO API) ─────────────────────────
def classify_post(text):
    """Return ('dimension', score) or ('none', 0). Higher score = stronger match."""
    if not text or len(text) < 10:
        return ('none', 0)
    text_lower = text.lower()
    scores = {}
    for dim, kws in DIMENSION_KEYWORDS.items():
        count = sum(1 for kw in kws if kw in text_lower)
        if count > 0:
            scores[dim] = count
    if not scores:
        return ('none', 0)
    best = max(scores.items(), key=lambda x: x[1])
    return best


def sentiment_score(text):
    if not text:
        return 0.0
    text_lower = text.lower()
    words = re.findall(r'[a-z]+', text_lower)
    if not words:
        return 0.0
    pos = sum(1 for w in words if w in POSITIVE_WORDS)
    neg = sum(1 for w in words if w in NEGATIVE_WORDS)
    if pos == 0 and neg == 0:
        return 0.0
    return (pos - neg) / max(pos + neg, 1)


# ── REACTION NAME → EMOJI ─────────────────────────────────────────
REACTION_TO_EMOJI = {
    'heart': '❤️', 'red_heart': '❤️', 'sparkling_heart': '💖',
    'fire': '🔥', 'clap': '👏', '+1': '👍', 'thumbsup': '👍',
    'thumbsdown': '👎', 'joy': '😂', 'sob': '😭',
    'heart_eyes': '😍', 'raised_hands': '🙌', 'pray': '🙏',
    'muscle': '💪', 'tada': '🎉', '100': '💯',
    'eyes': '👀', 'wave': '👋', 'star': '⭐', 'star2': '🌟',
    'rocket': '🚀', 'white_check_mark': '✅', 'x': '❌',
    'laughing': '😆', 'smile': '😄', 'grinning': '😀',
    'kissing_heart': '😘', 'blush': '😊', 'wink': '😉',
    'sunglasses': '😎', 'cry': '😢', 'scream': '😱', 'sweat_smile': '😅',
    'rolling_on_the_floor_laughing': '🤣', 'hugging_face': '🤗',
    'thinking_face': '🤔', 'face_with_hand_over_mouth': '🤭',
    'saluting_face': '🫡', 'melting_face': '🫠',
    'face_with_peeking_eye': '🫣', 'seedling': '🌱', 'herb': '🌿',
    'four_leaf_clover': '🍀', 'cherry_blossom': '🌸', 'sunflower': '🌻',
    'rose': '🌹', 'dog': '🐶', 'cat': '🐱', 'bear': '🧸',
    'yum': '😋', 'yellow_heart': '💛', 'purple_heart': '💜',
    'heart_hands': '🫶',
}

def reaction_to_display(name):
    return REACTION_TO_EMOJI.get(name, f":{name}:")


# ── SLACK API ─────────────────────────────────────────────────────
def get_channel_id(client, name):
    cursor = None
    while True:
        result = client.conversations_list(types="public_channel", limit=200, cursor=cursor)
        for ch in result["channels"]:
            if ch["name"] == name:
                return ch["id"]
        cursor = result.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    raise Exception(f"Channel '{name}' not found")


def get_users(client):
    result = client.users_list()
    users = {}
    for u in result["members"]:
        if not u["is_bot"] and not u["deleted"] and u["id"] != "USLACKBOT":
            users[u["id"]] = u.get("real_name") or u.get("name")
    return users


def get_all_messages(client, channel_id, oldest, latest):
    messages = []
    cursor = None
    while True:
        result = client.conversations_history(
            channel=channel_id, oldest=str(oldest), latest=str(latest),
            limit=200, cursor=cursor
        )
        messages.extend(result["messages"])
        cursor = result.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return messages


def get_replies(client, channel_id, ts, oldest, latest):
    try:
        result = client.conversations_replies(
            channel=channel_id, ts=ts, oldest=str(oldest), latest=str(latest),
            limit=200
        )
        return result["messages"][1:]
    except SlackApiError:
        return []


# ── CONFIG LOADER ─────────────────────────────────────────────────
def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", help='Month label like "April 2026" (auto-detect if blank)')
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = load_config()
    today = datetime.now().date()

    # Determine which month to wrap
    if args.month:
        month_label = args.month
    else:
        # Find month whose wrapped_post_date is today (or most recent past)
        month_label = None
        for label, info in config["months"].items():
            wpd = datetime.strptime(info["wrapped_post_date"], "%Y-%m-%d").date()
            if wpd == today:
                month_label = label
                break
        if not month_label:
            # Fallback: most recent month whose wrapped_post_date is in the past
            past_months = [
                (label, datetime.strptime(info["wrapped_post_date"], "%Y-%m-%d").date())
                for label, info in config["months"].items()
                if datetime.strptime(info["wrapped_post_date"], "%Y-%m-%d").date() <= today
            ]
            if past_months:
                month_label = max(past_months, key=lambda x: x[1])[0]
            else:
                print("❌ No completed month found. Use --month to specify.")
                sys.exit(1)

    if month_label not in config["months"]:
        print(f"❌ Month '{month_label}' not found in config")
        sys.exit(1)

    month_info = config["months"][month_label]
    start_date = datetime.strptime(month_info["start"], "%Y-%m-%d")
    end_date = datetime.strptime(month_info["end"], "%Y-%m-%d")
    end_exclusive = end_date + timedelta(days=1)

    print(f"🌱 Wrapped: {month_label} ({start_date.strftime('%b %-d')} – {end_date.strftime('%b %-d')})")
    if args.dry_run:
        print("🧪 DRY RUN")
    print()

    bot_client = WebClient(token=BOT_TOKEN)
    user_client = WebClient(token=USER_TOKEN)
    channel_name = config.get("channel", "zengarden")

    print("🔍 Fetching channel & users...")
    channel_id = get_channel_id(bot_client, channel_name)
    users = get_users(user_client)
    print(f"   Found {len(users)} users")

    oldest = start_date.timestamp()
    latest = end_exclusive.timestamp()

    # ── Per-user trackers ────────────────────────────────────────
    word_counts = defaultdict(Counter)
    emoji_typed = defaultdict(Counter)
    reactions_recv = defaultdict(Counter)
    reactions_given = defaultdict(Counter)
    photos_shared = defaultdict(int)
    post_hours = defaultdict(Counter)
    thread_pairs = defaultdict(Counter)
    max_reactions = defaultdict(int)
    message_count = defaultdict(int)
    post_count = defaultdict(int)  # top-level only
    reply_count = defaultdict(int)
    sentiment_sum = defaultdict(float)
    sentiment_n = defaultdict(int)
    dim_counts = defaultdict(Counter)  # uid -> Counter of dimensions

    print("📨 Fetching messages...")
    messages = get_all_messages(bot_client, channel_id, oldest, latest)
    print(f"   {len(messages)} top-level messages\n")

    print("⚙️  Processing...")
    thread_count = 0
    for msg in messages:
        if msg.get("subtype") in ("channel_join", "channel_leave", "bot_message"):
            continue
        uid = msg.get("user")
        if not uid or uid not in users:
            continue

        text = msg.get("text", "")
        post_count[uid] += 1
        message_count[uid] += 1

        # Words
        for w in extract_words(text):
            word_counts[uid][w] += 1

        # Emojis typed
        for e in extract_emojis(text):
            emoji_typed[uid][e] += 1

        # Photos
        for f in msg.get("files", []):
            if f.get("mimetype", "").startswith("image/"):
                photos_shared[uid] += 1

        # Hour
        ts = float(msg.get("ts", 0))
        dt = datetime.fromtimestamp(ts)
        post_hours[uid][dt.hour] += 1

        # Sentiment + dimension
        s = sentiment_score(text)
        sentiment_sum[uid] += s
        sentiment_n[uid] += 1
        dim, _ = classify_post(text)
        if dim != "none":
            dim_counts[uid][dim] += 1

        # Reactions on this post
        post_reaction_total = 0
        for reaction in msg.get("reactions", []):
            cnt = reaction["count"]
            reactions_recv[uid][reaction["name"]] += cnt
            post_reaction_total += cnt
            for ruid in reaction.get("users", []):
                if ruid in users:
                    reactions_given[ruid][reaction["name"]] += 1
        max_reactions[uid] = max(max_reactions[uid], post_reaction_total)

        # Threads
        if msg.get("reply_count", 0) > 0:
            thread_count += 1
            replies = get_replies(bot_client, channel_id, msg["ts"], oldest, latest)
            participants = {uid}
            for reply in replies:
                ruid = reply.get("user")
                if not ruid or ruid not in users:
                    continue
                rtext = reply.get("text", "")
                reply_count[ruid] += 1
                message_count[ruid] += 1
                participants.add(ruid)

                for w in extract_words(rtext):
                    word_counts[ruid][w] += 1
                for e in extract_emojis(rtext):
                    emoji_typed[ruid][e] += 1
                for f in reply.get("files", []):
                    if f.get("mimetype", "").startswith("image/"):
                        photos_shared[ruid] += 1

                rdt = datetime.fromtimestamp(float(reply.get("ts", 0)))
                post_hours[ruid][rdt.hour] += 1

                s = sentiment_score(rtext)
                sentiment_sum[ruid] += s
                sentiment_n[ruid] += 1

                for reaction in reply.get("reactions", []):
                    reactions_recv[ruid][reaction["name"]] += reaction["count"]
                    for r2 in reaction.get("users", []):
                        if r2 in users:
                            reactions_given[r2][reaction["name"]] += 1

            # Buddy pairs
            plist = list(participants)
            for i, p1 in enumerate(plist):
                for p2 in plist[i+1:]:
                    thread_pairs[p1][p2] += 1
                    thread_pairs[p2][p1] += 1

    print(f"   Processed {thread_count} threads\n")

    # ── Read weekly points for this month ────────────────────────
    print("💰 Loading weekly points...")
    weekly_data = {}
    if os.path.exists(WEEKLY_JSON):
        with open(WEEKLY_JSON) as f:
            weekly_data = json.load(f)

    # Get weeks for this month
    month_weeks = [w for w in weekly_data.get("weeks", []) if w.get("month") == month_label]
    month_weeks.sort(key=lambda w: w["week_number"])

    # Build per-person points map: name -> {wk1, wk2, wk3, wk4, pts}
    points_by_name = defaultdict(lambda: {"wk1": 0, "wk2": 0, "wk3": 0, "wk4": 0, "pts": 0})
    for week in month_weeks:
        wn = week["week_number"]
        for s in week["scores"]:
            points_by_name[s["name"]][f"wk{wn}"] = s["points"]
            points_by_name[s["name"]]["pts"] += s["points"]

    print(f"   {len(month_weeks)} weeks loaded, {len(points_by_name)} people scored")

    # Role map from config
    role_map_by_name = {}
    for role_name, names in config.get("roles", {}).items():
        for n in names:
            role_map_by_name[n.lower()] = role_name

    def get_role(name):
        return role_map_by_name.get(name.lower(), "RBT")

    # ── Bonus tier logic ──────────────────────────────────────────
    def bonus_tier(pts):
        if pts >= 80: return "$50"
        if pts >= 50: return "$25"
        if pts >= 25: return "$15"
        return "$0"

    # ── Style classifier (Pure Engager / Engager / Balanced / Broadcaster / Heavy Broadcaster)
    def classify_style(posts, replies):
        total = posts + replies
        if total == 0:
            return "Balanced"
        post_ratio = posts / total
        if post_ratio >= 0.7:
            return "Heavy Broadcaster"
        if post_ratio >= 0.5:
            return "Broadcaster"
        if post_ratio >= 0.3:
            return "Balanced"
        if post_ratio >= 0.15:
            return "Engager"
        return "Pure Engager"

    # ── BUILD all_active ─────────────────────────────────────────
    print("\n📊 Building wrapped data...\n")
    all_active = []

    for uid, name in users.items():
        if message_count[uid] == 0:
            continue

        pts_data = points_by_name.get(name, {"wk1": 0, "wk2": 0, "wk3": 0, "wk4": 0, "pts": 0})
        wk1, wk2, wk3, wk4 = pts_data["wk1"], pts_data["wk2"], pts_data["wk3"], pts_data["wk4"]
        pts = pts_data["pts"]

        # Streak: count weeks > 0
        streak = sum(1 for x in (wk1, wk2, wk3, wk4) if x > 0)

        # Trend: compare wk1 to wk4
        if wk4 > wk1:
            trend = "Up"
        elif wk4 < wk1:
            trend = "Down"
        else:
            trend = "Steady"

        # Style
        style = classify_style(post_count[uid], reply_count[uid])

        # Top words/emojis/reactions
        tw = word_counts[uid].most_common(5)
        te = [e[0] for e in emoji_typed[uid].most_common(3)]
        tr = [reaction_to_display(r[0]) for r in reactions_recv[uid].most_common(3)]
        tg = [reaction_to_display(r[0]) for r in reactions_given[uid].most_common(3)]

        # Hour
        hours = post_hours[uid]
        if hours:
            top_hour = hours.most_common(1)[0][0]
            active_lbl = hour_label(top_hour)
        else:
            top_hour = -1
            active_lbl = ""

        # Bestie
        buddy = thread_pairs[uid]
        bestie_uid = buddy.most_common(1)[0][0] if buddy else None
        bestie = ""
        if bestie_uid:
            full = users.get(bestie_uid, "")
            bestie = full.split(" ")[0] if full else ""

        # Dimension
        my_dims = dim_counts[uid]
        big_dim = my_dims.most_common(1)[0][0].title() if my_dims else "Social"
        wellness_total = sum(my_dims.values())

        # Sentiment
        avg_sent = sentiment_sum[uid] / sentiment_n[uid] if sentiment_n[uid] else 0.0

        # Encourage = total reactions given to OTHERS
        encourage = sum(reactions_given[uid].values())

        # Conn = number of unique thread co-participants weighted by frequency
        conn = sum(buddy.values())

        # Stress = posts with negative sentiment
        # (rough: count from sentiment_n where text leaned negative)
        # Simpler: stress flag if avg_sentiment < -0.1
        stress = 1 if avg_sent < -0.1 else 0

        all_active.append({
            "name": name,
            "role": get_role(name),
            "pts": pts,
            "bonus": bonus_tier(pts),
            "streak": streak,
            "ci": 100,
            "dim": big_dim,
            "wellness": wellness_total,
            "stress": stress,
            "encourage": encourage,
            "conn": conn,
            "style": style,
            "trend": trend,
            "wk1": wk1, "wk2": wk2, "wk3": wk3, "wk4": wk4,
            "sentiment": round(avg_sent, 2),
            "top_words": tw,
            "top_emojis": te,
            "top_reactions": tr,
            "reactions_given": tg,
            "photos_shared": photos_shared[uid],
            "active_hour": top_hour,
            "active_label": active_lbl,
            "bestie": bestie,
            "most_hyped": max_reactions[uid],
        })

    # Sort by points desc
    all_active.sort(key=lambda x: -x["pts"])

    # ── AGGREGATES ────────────────────────────────────────────────
    total_users = len(users)
    active = len(all_active)
    participation_rate = round((active / total_users * 100), 1) if total_users else 0
    total_pts = sum(p["pts"] for p in all_active)
    total_payout = sum({"$50": 50, "$25": 25, "$15": 15, "$0": 0}[p["bonus"]] for p in all_active)
    total_posts_overall = sum(post_count[uid] for uid in post_count)
    total_comments_overall = sum(reply_count[uid] for uid in reply_count)

    # Role averages
    role_buckets = defaultdict(list)
    for p in all_active:
        role_buckets[p["role"]].append(p["pts"])
    bcba_avg = round(sum(role_buckets["BCBA"]) / len(role_buckets["BCBA"]), 2) if role_buckets["BCBA"] else 0
    rbt_avg = round(sum(role_buckets["RBT"]) / len(role_buckets["RBT"]), 2) if role_buckets["RBT"] else 0
    lead_avg = round(sum(role_buckets["Leadership"]) / len(role_buckets["Leadership"]), 2) if role_buckets["Leadership"] else 0

    # Weekly totals
    wk_totals = [
        sum(p["wk1"] for p in all_active),
        sum(p["wk2"] for p in all_active),
        sum(p["wk3"] for p in all_active),
        sum(p["wk4"] for p in all_active),
    ]

    # Dimension counts (global)
    global_dims = Counter()
    for p in all_active:
        for u_dim, c in dim_counts[next(uid for uid, n in users.items() if n == p["name"])].items():
            global_dims[u_dim.title()] += c

    # Sentiment aggregate
    all_sentiments = [p["sentiment"] for p in all_active if p["sentiment"] != 0]
    avg_sentiment = round(sum(all_sentiments) / len(all_sentiments), 2) if all_sentiments else 0
    positive_pct = round(sum(1 for s in all_sentiments if s > 0) / len(all_sentiments) * 100, 1) if all_sentiments else 0
    stress_flags = sum(1 for p in all_active if p["stress"])

    # Style counts
    style_counts = Counter(p["style"] for p in all_active)

    # Global word counts (top 13)
    global_words = Counter()
    for uid, c in word_counts.items():
        global_words.update(c)
    top_global_words = global_words.most_common(13)

    # ── AWARDS ────────────────────────────────────────────────────
    awards = {}

    if all_active:
        streaker = max(all_active, key=lambda p: (p["streak"], p["pts"]))
        awards["longest_streak"] = {"name": streaker["name"], "value": streaker["streak"]}

        improved = max(all_active, key=lambda p: p["wk4"] - p["wk1"])
        awards["most_improved"] = {
            "name": improved["name"],
            "delta": improved["wk4"] - improved["wk1"],
            "wk1": improved["wk1"], "wk4": improved["wk4"],
        }

        loved = max(all_active, key=lambda p: p["conn"])
        awards["most_loved"] = {"name": loved["name"], "value": loved["conn"]}

        hype = max(all_active, key=lambda p: p["encourage"])
        awards["hype_machine"] = {"name": hype["name"], "value": hype["encourage"]}

        motiv = max(all_active, key=lambda p: p["wellness"])
        awards["most_motivating"] = {"name": motiv["name"], "value": motiv["wellness"]}

        unsung_candidates = [p for p in all_active if p["pts"] < 80 and p["conn"] > 15]
        if unsung_candidates:
            unsung = max(unsung_candidates, key=lambda p: p["conn"])
        else:
            unsung = all_active[min(8, len(all_active)-1)]
        awards["silent_mvp"] = {"name": unsung["name"], "value": unsung["conn"]}

        photo = max(all_active, key=lambda p: p["photos_shared"])
        awards["photo_energy"] = {"name": photo["name"], "value": photo["photos_shared"]}

        support_candidates = [p for p in all_active if p["pts"] >= 50]
        if support_candidates:
            support = max(support_candidates, key=lambda p: p["conn"])
        else:
            support = all_active[0]
        awards["most_supportive"] = {"name": support["name"], "value": support["conn"]}

        # Best Duo: top 2 in conn
        duos = sorted(all_active, key=lambda p: -p["conn"])[:2]
        if len(duos) >= 2:
            awards["best_duo"] = {"name1": duos[0]["name"], "name2": duos[1]["name"]}

    # ── Insights ──────────────────────────────────────────────────
    word_counts_global = top_global_words

    output = {
        "month": month_label,
        "date_range": f"{start_date.strftime('%b %-d')} – {end_date.strftime('%b %-d')}",
        "total": total_users,
        "active": active,
        "participation_rate": participation_rate,
        "total_pts": total_pts,
        "total_payout": total_payout,
        "total_posts": total_posts_overall,
        "total_comments": total_comments_overall,
        "bcba_avg": bcba_avg,
        "rbt_avg": rbt_avg,
        "lead_avg": lead_avg,
        "wk_totals": wk_totals,
        "dim_counts": dict(global_dims),
        "avg_sentiment": avg_sentiment,
        "positive_pct": positive_pct,
        "stress_flags": stress_flags,
        "styles": dict(style_counts),
        "word_counts": word_counts_global,
        "all_active": all_active,
        "awards": awards,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    # Print summary
    print(f"\n  Active: {active}/{total_users} ({participation_rate}%)")
    print(f"  Total points: {total_pts}")
    print(f"  Total payout: ${total_payout}")
    print(f"  Avg sentiment: {avg_sentiment}")
    print(f"  Top dimension: {global_dims.most_common(1)[0][0] if global_dims else '-'}")
    print(f"\n  Top 3 by points:")
    for i, p in enumerate(all_active[:3], 1):
        print(f"    {i}. {p['name']} — {p['pts']} pts ({p['style']})")
    print(f"\n  Awards:")
    for a, info in awards.items():
        print(f"    {a}: {info}")

    if args.dry_run:
        print("\n🧪 DRY RUN — not saving")
        return

    with open(WRAPPED_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved → {WRAPPED_JSON}")


if __name__ == "__main__":
    main()
