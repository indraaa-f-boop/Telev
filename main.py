import asyncio
import random
import time
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from telegram.error import TelegramError, NetworkError, TimedOut, BadRequest

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Konfigurasi Bot
BOT_TOKEN = "8461616896:AAFOfTm54k54G8kQVQf6nqSVVYClO6Z5EQg"
OWNER_ID = 5802965692

# Database configuration
DB_NAME = "hiragana_bot.db"

# Data Hiragana lengkap dengan romaji
HIRAGANA_DATA = {
    'あ': 'a', 'い': 'i', 'う': 'u', 'え': 'e', 'お': 'o',
    'か': 'ka', 'き': 'ki', 'く': 'ku', 'け': 'ke', 'こ': 'ko',
    'が': 'ga', 'ぎ': 'gi', 'ぐ': 'gu', 'げ': 'ge', 'ご': 'go',
    'さ': 'sa', 'し': 'shi', 'す': 'su', 'せ': 'se', 'そ': 'so',
    'ざ': 'za', 'じ': 'ji', 'ず': 'zu', 'ぜ': 'ze', 'ぞ': 'zo',
    'た': 'ta', 'ち': 'chi', 'つ': 'tsu', 'て': 'te', 'と': 'to',
    'だ': 'da', 'ぢ': 'di', 'づ': 'du', 'で': 'de', 'ど': 'do',
    'な': 'na', 'に': 'ni', 'ぬ': 'nu', 'ね': 'ne', 'の': 'no',
    'は': 'ha', 'ひ': 'hi', 'ふ': 'fu', 'へ': 'he', 'ほ': 'ho',
    'ば': 'ba', 'び': 'bi', 'ぶ': 'bu', 'べ': 'be', 'ぼ': 'bo',
    'ぱ': 'pa', 'ぴ': 'pi', 'ぷ': 'pu', 'ぺ': 'pe', 'ぽ': 'po',
    'ま': 'ma', 'み': 'mi', 'む': 'mu', 'め': 'me', 'も': 'mo',
    'や': 'ya', 'ゆ': 'yu', 'よ': 'yo',
    'ら': 'ra', 'り': 'ri', 'る': 'ru', 'れ': 're', 'ろ': 'ro',
    'わ': 'wa', 'ゐ': 'wi', 'ゑ': 'we', 'を': 'wo', 'ん': 'n'
}

# Reverse mapping untuk mencari hiragana dari romaji
ROMAJI_TO_HIRAGANA = {v: k for k, v in HIRAGANA_DATA.items()}

# Pembagian level yang lebih seimbang
LEVELS = {
    1: ['あ', 'い', 'う', 'え', 'お', 'か', 'き', 'く', 'け', 'こ', 'さ', 'し'],
    2: ['す', 'せ', 'そ', 'た', 'ち', 'つ', 'て', 'と', 'な', 'に', 'ぬ', 'ね'],
    3: ['の', 'は', 'ひ', 'ふ', 'へ', 'ほ', 'ま', 'み', 'む', 'め', 'も', 'や'],
    4: ['ゆ', 'よ', 'ら', 'り', 'る', 'れ', 'ろ', 'わ', 'を', 'ん', 'が', 'ぎ']
}

# Storage untuk quiz session dan user data
quiz_sessions: Dict[int, Dict] = {}
user_statistics: Dict[int, Dict] = {}
used_romaji: Dict[int, List[str]] = {}

class PremiumHiraganaQuizBot:
    def __init__(self):
        self.application = Application.builder().token(BOT_TOKEN).build()
        self.setup_database()
        self.setup_handlers()
        self.setup_error_handling()
    
    def setup_database(self):
        """Setup database SQLite untuk data yang lebih robust"""
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            
            # Tabel users dengan informasi tambahan
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_bot INTEGER DEFAULT 0,
                    language_code TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_active DATETIME DEFAULT CURRENT_TIMESTAMP,
                    total_messages INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'active'
                )
            ''')
            
            # Tabel user_stats dengan detail lengkap
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    total_games INTEGER DEFAULT 0,
                    total_questions INTEGER DEFAULT 0,
                    total_correct INTEGER DEFAULT 0,
                    best_score_level1 INTEGER DEFAULT 0,
                    best_score_level2 INTEGER DEFAULT 0,
                    best_score_level3 INTEGER DEFAULT 0,
                    best_score_level4 INTEGER DEFAULT 0,
                    level1_plays INTEGER DEFAULT 0,
                    level2_plays INTEGER DEFAULT 0,
                    level3_plays INTEGER DEFAULT 0,
                    level4_plays INTEGER DEFAULT 0,
                    easy_games INTEGER DEFAULT 0,
                    hard_games INTEGER DEFAULT 0,
                    easy_correct INTEGER DEFAULT 0,
                    hard_correct INTEGER DEFAULT 0,
                    easy_total INTEGER DEFAULT 0,
                    hard_total INTEGER DEFAULT 0,
                    total_time_played INTEGER DEFAULT 0,
                    average_score REAL DEFAULT 0.0,
                    best_streak INTEGER DEFAULT 0,
                    current_streak INTEGER DEFAULT 0,
                    first_play DATETIME,
                    last_play DATETIME,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Tabel game_history untuk track record detail
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS game_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    level INTEGER,
                    mode TEXT,
                    score INTEGER,
                    total_questions INTEGER,
                    percentage REAL,
                    duration INTEGER,
                    grade TEXT,
                    questions_data TEXT,
                    answers_data TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Tabel user_progress untuk tracking pembelajaran
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    hiragana_char TEXT,
                    romaji TEXT,
                    correct_count INTEGER DEFAULT 0,
                    wrong_count INTEGER DEFAULT 0,
                    last_seen DATETIME,
                    mastery_level INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database setup completed successfully")
        except Exception as e:
            logger.error(f"Database setup error: {e}")
    
    def setup_handlers(self):
        """Setup semua handler untuk bot"""
        handlers = [
            CommandHandler("start", self.start_command),
            CommandHandler("stats", self.stats_command),
            CommandHandler("help", self.help_command),
            CommandHandler("adminstats", self.admin_stats_command),
            CommandHandler("userlist", self.user_list_command),
            CommandHandler("userinfo", self.user_info_command),
            CommandHandler("broadcast", self.broadcast_command),
            CommandHandler("gamehistory", self.game_history_command),
            CallbackQueryHandler(self.button_callback),
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_broadcast_message)
        ]
        
        for handler in handlers:
            self.application.add_handler(handler)
    
    def setup_error_handling(self):
        """Setup error handling premium"""
        self.application.add_error_handler(self.error_handler)
    
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle errors dengan logging yang lebih detail"""
        logger.error(f"Exception: {context.error}", exc_info=True)
        
        if isinstance(context.error, (NetworkError, TimedOut)):
            logger.info("Network error - retrying...")
            await asyncio.sleep(2)
        elif isinstance(context.error, BadRequest):
            if "query is too old" in str(context.error).lower():
                logger.info("Callback query expired - ignoring")
                return
            elif "message to edit not found" in str(context.error).lower():
                logger.info("Message to edit not found - ignoring")
                return
    
    async def safe_answer_callback(self, query):
        """Jawab callback query dengan aman (handle expired queries)"""
        try:
            await query.answer()
            return True
        except BadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.info(f"Callback query expired or invalid: {e}")
                return False
            else:
                logger.error(f"Callback answer error: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected callback error: {e}")
            return False
    
    async def safe_edit_message(self, query, text, reply_markup=None, parse_mode='Markdown'):
        """Edit message dengan safe handling"""
        try:
            await query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            return True
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                logger.info("Message content is the same - no edit needed")
                return True
            elif "message to edit not found" in str(e).lower():
                logger.warning("Message to edit not found")
                # Send new message instead
                try:
                    await self.application.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode=parse_mode
                    )
                    return True
                except Exception as e2:
                    logger.error(f"Failed to send replacement message: {e2}")
                    return False
            else:
                logger.error(f"Edit message error: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected edit message error: {e}")
            return False
    
    def get_db_connection(self):
        """Dapatkan koneksi database"""
        return sqlite3.connect(DB_NAME)
    
    def init_user(self, user_id: int, username: str, first_name: str, last_name: str = "", language_code: str = ""):
        """Inisialisasi user baru di database"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Update user info
            cursor.execute('''
                INSERT OR REPLACE INTO users 
                (user_id, username, first_name, last_name, language_code, last_active, total_messages)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 
                        COALESCE((SELECT total_messages FROM users WHERE user_id = ?), 0) + 1)
            ''', (user_id, username, first_name, last_name, language_code, user_id))
            
            # Initialize user stats if not exists
            cursor.execute('SELECT user_id FROM user_stats WHERE user_id = ?', (user_id,))
            if not cursor.fetchone():
                cursor.execute('''
                    INSERT INTO user_stats (user_id, first_play, last_play)
                    VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', (user_id,))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error initializing user: {e}")
    
    def get_user_stats(self, user_id: int) -> Dict:
        """Dapatkan statistik user dari database"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'user_id': result[1],
                    'total_games': result[2] or 0,
                    'total_questions': result[3] or 0,
                    'total_correct': result[4] or 0,
                    'best_scores': {
                        1: result[5] or 0, 2: result[6] or 0, 3: result[7] or 0, 4: result[8] or 0
                    },
                    'level_plays': {
                        1: result[9] or 0, 2: result[10] or 0, 3: result[11] or 0, 4: result[12] or 0
                    },
                    'mode_stats': {
                        'easy': {
                            'games': result[13] or 0,
                            'correct': result[15] or 0,
                            'total': result[17] or 0
                        },
                        'hard': {
                            'games': result[14] or 0,
                            'correct': result[16] or 0,
                            'total': result[18] or 0
                        }
                    },
                    'total_time_played': result[19] or 0,
                    'average_score': result[20] or 0.0,
                    'best_streak': result[21] or 0,
                    'current_streak': result[22] or 0,
                    'first_play': result[23] or datetime.now().isoformat(),
                    'last_play': result[24] or datetime.now().isoformat()
                }
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
        
        return self.create_default_stats()
    
    def create_default_stats(self) -> Dict:
        """Buat statistik default"""
        return {
            'total_games': 0,
            'total_questions': 0,
            'total_correct': 0,
            'best_scores': {1: 0, 2: 0, 3: 0, 4: 0},
            'level_plays': {1: 0, 2: 0, 3: 0, 4: 0},
            'mode_stats': {
                'easy': {'games': 0, 'correct': 0, 'total': 0},
                'hard': {'games': 0, 'correct': 0, 'total': 0}
            },
            'total_time_played': 0,
            'average_score': 0.0,
            'best_streak': 0,
            'current_streak': 0,
            'first_play': datetime.now().isoformat(),
            'last_play': datetime.now().isoformat()
        }
    
    def update_user_stats(self, user_id: int, quiz_result: Dict):
        """Update statistik user setelah quiz selesai"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            stats = self.get_user_stats(user_id)
            
            # Update basic stats
            total_games = stats['total_games'] + 1
            total_questions = stats['total_questions'] + quiz_result['total']
            total_correct = stats['total_correct'] + quiz_result['score']
            
            # Calculate average score
            new_average = total_correct / total_questions if total_questions > 0 else 0
            
            # Update level specific stats
            level = quiz_result['level']
            best_score = max(stats['best_scores'][level], quiz_result['score'])
            level_plays = stats['level_plays'][level] + 1
            
            # Update mode specific stats
            mode = quiz_result['mode']
            mode_games = stats['mode_stats'][mode]['games'] + 1
            mode_correct = stats['mode_stats'][mode]['correct'] + quiz_result['score']
            mode_total = stats['mode_stats'][mode]['total'] + quiz_result['total']
            
            # Update time played
            total_time = stats['total_time_played'] + quiz_result.get('duration_seconds', 0)
            
            # Update streak
            if quiz_result['score'] == quiz_result['total']:
                current_streak = stats['current_streak'] + 1
                best_streak = max(stats['best_streak'], current_streak)
            else:
                current_streak = 0
                best_streak = stats['best_streak']
            
            cursor.execute(f'''
                UPDATE user_stats SET 
                total_games = ?, total_questions = ?, total_correct = ?,
                best_score_level{level} = ?, level{level}_plays = ?,
                {mode}_games = ?, {mode}_correct = ?, {mode}_total = ?,
                total_time_played = ?, average_score = ?, best_streak = ?, current_streak = ?,
                last_play = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (total_games, total_questions, total_correct, best_score, level_plays,
                 mode_games, mode_correct, mode_total, total_time, new_average, 
                 best_streak, current_streak, user_id))
            
            # Save detailed game history
            cursor.execute('''
                INSERT INTO game_history 
                (user_id, level, mode, score, total_questions, percentage, duration, grade, 
                 questions_data, answers_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, level, mode, quiz_result['score'], quiz_result['total'],
                  quiz_result['percentage'], quiz_result.get('duration_seconds', 0),
                  quiz_result['grade'], 
                  json.dumps(quiz_result.get('questions', [])),
                  json.dumps(quiz_result.get('answers', []))))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating user stats: {e}")
    
    def get_user_info(self, user):
        """Mendapatkan informasi user dengan aman"""
        return {
            'id': user.id,
            'username': getattr(user, 'username', 'Unknown') or 'Unknown',
            'first_name': getattr(user, 'first_name', 'Unknown') or 'Unknown',
            'last_name': getattr(user, 'last_name', '') or '',
            'full_name': f"{getattr(user, 'first_name', 'Unknown') or 'Unknown'} {getattr(user, 'last_name', '') or ''}".strip(),
            'language_code': getattr(user, 'language_code', '') or ''
        }
    
    def get_romaji_image_path(self, romaji: str) -> Optional[str]:
        """Mendapatkan path gambar untuk romaji tertentu"""
        image_mapping = {}
        for hiragana, romaji_val in HIRAGANA_DATA.items():
            image_mapping[romaji_val] = f"{romaji_val}.jpg"
        
        filename = image_mapping.get(romaji)
        if filename and os.path.exists(filename):
            return filename
        return None
    
    async def send_quiz_with_image(self, chat_id: int, message_text: str, romaji: str, reply_markup: InlineKeyboardMarkup):
        """Mengirim quiz dengan gambar romaji dalam satu pesan"""
        try:
            image_path = self.get_romaji_image_path(romaji)
            if image_path:
                with open(image_path, 'rb') as photo:
                    await self.application.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo,
                        caption=message_text,
                        reply_markup=reply_markup,
                        parse_mode='Markdown'
                    )
                return True
            else:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                return False
        except Exception as e:
            logger.error(f"Error sending quiz with image: {e}")
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            return False

    async def send_premium_track_record(self, user_info: dict, quiz_result: dict):
        """Mengirim track record ke owner"""
        try:
            track_message = f"""
🎯 **💎 TRACK RECORD 💎**

👤 **User Info:**
• ID: `{user_info['id']}`
• Username: @{user_info['username']}
• Name: {user_info['full_name']}
• Language: {user_info.get('language_code', 'unknown')}

📊 **Quiz Performance:**
• Level: {quiz_result['level']} • Mode: {quiz_result['mode'].title()}
• Score: {quiz_result['score']}/{quiz_result['total']} ({quiz_result['percentage']:.1f}%)
• Grade: {quiz_result['grade']} • Time: {quiz_result['duration']}

📈 **User Statistics:**
• Total Games: {quiz_result['user_stats']['total_games']}
• Best Score Lv{quiz_result['level']}: {quiz_result['user_stats']['best_scores'][quiz_result['level']]}/13
• Level {quiz_result['level']} Plays: {quiz_result['user_stats']['level_plays'][quiz_result['level']]}
• Current Streak: {quiz_result['user_stats']['current_streak']}
• Best Streak: {quiz_result['user_stats']['best_streak']}

⏰ **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            await self.application.bot.send_message(
                chat_id=OWNER_ID,
                text=track_message,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"Failed to send premium track record: {e}")

    # OWNER COMMANDS
    async def user_list_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command untuk melihat daftar user (owner only)"""
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Access denied.")
            return
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT u.user_id, u.username, u.first_name, u.last_name, 
                       u.created_at, u.last_active, u.total_messages,
                       s.total_games, s.total_questions, s.total_correct
                FROM users u
                LEFT JOIN user_stats s ON u.user_id = s.user_id
                ORDER BY u.last_active DESC
                LIMIT 20
            ''')
            
            users = cursor.fetchall()
            conn.close()
            
            if not users:
                await update.message.reply_text("📝 No users found.")
                return
            
            user_list_text = "👥 **Recent Active Users (Top 20):**\n\n"
            
            for user in users:
                user_id, username, first_name, last_name, created_at, last_active, total_msgs, total_games, total_q, total_c = user
                
                full_name = f"{first_name or 'Unknown'} {last_name or ''}".strip()
                username_display = f"@{username}" if username else "No username"
                accuracy = (total_c / total_q * 100) if total_q and total_q > 0 else 0
                
                user_list_text += f"""
**{full_name}**
• ID: `{user_id}`
• Username: {username_display}
• Games: {total_games or 0} | Accuracy: {accuracy:.1f}%
• Messages: {total_msgs or 0}
• Last Active: {last_active[:10] if last_active else 'Unknown'}
                """
            
            await update.message.reply_text(user_list_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error in user_list_command: {e}")
            await update.message.reply_text("❌ Error retrieving user list.")

    async def user_info_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command untuk melihat info detail user (owner only)"""
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Access denied.")
            return
        
        if not context.args:
            await update.message.reply_text("📝 Usage: /userinfo <user_id>")
            return
        
        try:
            user_id = int(context.args[0])
            
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get user info
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            user_data = cursor.fetchone()
            
            if not user_data:
                await update.message.reply_text("❌ User not found.")
                return
            
            # Get user stats
            cursor.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
            stats_data = cursor.fetchone()
            
            # Get recent games
            cursor.execute('''
                SELECT level, mode, score, total_questions, percentage, grade, timestamp
                FROM game_history WHERE user_id = ? 
                ORDER BY timestamp DESC LIMIT 5
            ''', (user_id,))
            recent_games = cursor.fetchall()
            
            conn.close()
            
            # Format user info
            username = f"@{user_data[1]}" if user_data[1] else "No username"
            full_name = f"{user_data[2] or 'Unknown'} {user_data[3] or ''}".strip()
            
            user_info_text = f"""
👤 **User Profile: {full_name}**

**Basic Info:**
• ID: `{user_id}`
• Username: {username}
• Language: {user_data[5] or 'Unknown'}
• Registered: {user_data[6][:10] if user_data[6] else 'Unknown'}
• Last Active: {user_data[7][:10] if user_data[7] else 'Unknown'}
• Total Messages: {user_data[8] or 0}
• Status: {user_data[9] or 'active'}

**Game Statistics:**"""
            
            if stats_data:
                total_games = stats_data[2] or 0
                total_questions = stats_data[3] or 0
                total_correct = stats_data[4] or 0
                accuracy = (total_correct / total_questions * 100) if total_questions > 0 else 0
                
                user_info_text += f"""
• Total Games: {total_games}
• Total Questions: {total_questions}
• Overall Accuracy: {accuracy:.1f}%
• Time Played: {(stats_data[19] or 0) // 60}m {(stats_data[19] or 0) % 60}s
• Best Streak: {stats_data[21] or 0}
• Current Streak: {stats_data[22] or 0}

**Level Best Scores:**
• Level 1: {stats_data[5] or 0}/13
• Level 2: {stats_data[6] or 0}/13
• Level 3: {stats_data[7] or 0}/13
• Level 4: {stats_data[8] or 0}/13

**Recent Games:**"""
                
                if recent_games:
                    for game in recent_games:
                        level, mode, score, total_q, percentage, grade, timestamp = game
                        user_info_text += f"""
• Lv{level} {mode}: {score}/{total_q} ({percentage:.1f}%) {grade} - {timestamp[:10]}"""
                else:
                    user_info_text += "\n• No recent games"
            else:
                user_info_text += "\n• No game statistics available"
            
            await update.message.reply_text(user_info_text, parse_mode='Markdown')
            
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID format.")
        except Exception as e:
            logger.error(f"Error in user_info_command: {e}")
            await update.message.reply_text("❌ Error retrieving user info.")

    async def game_history_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command untuk melihat history game user (owner only)"""
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Access denied.")
            return
        
        if not context.args:
            await update.message.reply_text("📝 Usage: /gamehistory <user_id> [limit]")
            return
        
        try:
            user_id = int(context.args[0])
            limit = int(context.args[1]) if len(context.args) > 1 else 10
            limit = min(limit, 50)  # Max 50 records
            
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get user name first
            cursor.execute('SELECT first_name, last_name FROM users WHERE user_id = ?', (user_id,))
            user_data = cursor.fetchone()
            
            if not user_data:
                await update.message.reply_text("❌ User not found.")
                return
            
            full_name = f"{user_data[0] or 'Unknown'} {user_data[1] or ''}".strip()
            
            # Get game history
            cursor.execute('''
                SELECT level, mode, score, total_questions, percentage, grade, duration, timestamp
                FROM game_history WHERE user_id = ? 
                ORDER BY timestamp DESC LIMIT ?
            ''', (user_id, limit))
            
            games = cursor.fetchall()
            conn.close()
            
            if not games:
                await update.message.reply_text(f"📝 No game history found for {full_name}.")
                return
            
            history_text = f"🎮 **Game History for {full_name}**\n`User ID: {user_id}`\n\n"
            
            for i, game in enumerate(games, 1):
                level, mode, score, total_q, percentage, grade, duration, timestamp = game
                duration_text = f"{duration//60}m {duration%60}s" if duration else "N/A"
                
                history_text += f"""
**{i}. Level {level} - {mode.title()} Mode**
• Score: {score}/{total_q} ({percentage:.1f}%)
• Grade: {grade}
• Duration: {duration_text}
• Date: {timestamp[:16] if timestamp else 'Unknown'}
"""
            
            await update.message.reply_text(history_text, parse_mode='Markdown')
            
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID or limit format.")
        except Exception as e:
            logger.error(f"Error in game_history_command: {e}")
            await update.message.reply_text("❌ Error retrieving game history.")

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command untuk broadcast message ke semua user (owner only)"""
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Access denied.")
            return
        
        if not context.args:
            await update.message.reply_text("📢 Usage: /broadcast <message>\nExample: /broadcast Hello everyone! New features available!")
            return
        
        message = ' '.join(context.args)
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT user_id FROM users WHERE status = "active"')
            users = cursor.fetchall()
            conn.close()
            
            if not users:
                await update.message.reply_text("📝 No active users found.")
                return
            
            success_count = 0
            failed_count = 0
            
            broadcast_message = f"📢 **BROADCAST MESSAGE**\n\n{message}\n\n---\nFrom: Hiragana Master Pro Admin"
            
            status_msg = await update.message.reply_text(f"📡 Starting broadcast to {len(users)} users...")
            
            for user_tuple in users:
                user_id = user_tuple[0]
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=broadcast_message,
                        parse_mode='Markdown'
                    )
                    success_count += 1
                    
                    # Update progress every 10 users
                    if success_count % 10 == 0:
                        await status_msg.edit_text(
                            f"📡 Broadcasting... {success_count}/{len(users)} sent"
                        )
                    
                    await asyncio.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    failed_count += 1
                    logger.warning(f"Failed to send broadcast to {user_id}: {e}")
            
            final_message = f"""
✅ **Broadcast Complete!**

📊 **Results:**
• Total Users: {len(users)}
• Successfully Sent: {success_count}
• Failed: {failed_count}
• Success Rate: {(success_count/len(users)*100):.1f}%
            """
            
            await status_msg.edit_text(final_message, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error in broadcast_command: {e}")
            await update.message.reply_text("❌ Error during broadcast.")

    async def handle_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages untuk broadcast mode"""
        # Hanya track user activity, tidak perlu response khusus
        if update.effective_user:
            user_info = self.get_user_info(update.effective_user)
            self.init_user(
                update.effective_user.id,
                user_info['username'],
                user_info['first_name'],
                user_info['last_name'],
                user_info['language_code']
            )

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler premium untuk command /start"""
        try:
            user = update.effective_user
            user_info = self.get_user_info(user)
            
            self.init_user(
                user.id, 
                user_info['username'], 
                user_info['first_name'], 
                user_info['last_name'],
                user_info['language_code']
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("🌱 Level 1 (Basic)", callback_data="level_1"),
                    InlineKeyboardButton("🌿 Level 2 (Easy)", callback_data="level_2")
                ],
                [
                    InlineKeyboardButton("🌳 Level 3 (Medium)", callback_data="level_3"),
                    InlineKeyboardButton("🌸 Level 4 (Advanced)", callback_data="level_4")
                ],
                [
                    InlineKeyboardButton("📊 My Statistics", callback_data="my_stats"),
                    InlineKeyboardButton("💡 How to Play", callback_data="help")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            user_stats = self.get_user_stats(user.id)
            welcome_text = f"""
💎 **Hiragana Master Pro** 💎

*Welcome {user_info['first_name']}!* 🎉

🚀 **Features:**
• 4 Progressive Learning Levels
• Smart Adaptive Quiz System  
• Real-time Progress Tracking
• Advanced Performance Analytics

🎯 **Learning Path:**
1. **Level 1** - Basic Characters (あ-し)
2. **Level 2** - Intermediate (す-ね)
3. **Level 3** - Advanced (の-や)
4. **Level 4** - Expert (ゆ-ぎ)

📊 **Your Current Status:**
• Total Games: {user_stats['total_games']}
• Best Accuracy: {(user_stats['total_correct']/user_stats['total_questions']*100) if user_stats['total_questions'] > 0 else 0:.1f}%

*Ready to master Hiragana? Choose your level!* 🎓
            """
            
            await update.message.reply_text(
                text=welcome_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"Error in start_command: {e}")
            await update.message.reply_text("❌ System error. Please try again.")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler untuk command /stats"""
        try:
            user_id = update.effective_user.id
            await self.show_user_stats(update.effective_chat.id, user_id)
        except Exception as e:
            logger.error(f"Error in stats_command: {e}")
            await update.message.reply_text("❌ Error retrieving statistics.")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler untuk command /help"""
        help_text = """
💎 **Hiragana Master Pro - Guide** 💎

📚 **How to Play:**
1. Choose level (1-4) based on your skill
2. Select mode: Easy (multiple choice) or Hard (true/false)
3. Answer 13 questions in 3 minutes
4. Get your score and grade!
5. View romaji illustrations for each sound

🎯 **Levels:**
• Level 1: Basic characters (あ-し)
• Level 2: Intermediate (す-ね)
• Level 3: Advanced (の-や)  
• Level 4: Expert (ゆ-ぎ)

🏆 **Grading System:**
• 95-100%: 💎 Master
• 85-94%: 🏆 Excellent
• 75-84%: ⭐ Great
• 60-74%: 👍 Good
• <60%: 💪 Practice Needed

📊 **Commands:**
• /start - Main menu
• /stats - Your statistics
• /help - This guide

🖼️ **New Feature:**
Each question shows ROMAJI illustration, you choose the correct HIRAGANA character!

🤖 Happy learning! 頑張って！
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')

    async def admin_stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command untuk melihat statistik global (owner only)"""
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Access denied.")
            return
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Basic statistics
            cursor.execute('SELECT COUNT(*) FROM users')
            total_users = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM user_stats WHERE DATE(last_play) = DATE("now")')
            active_today = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM user_stats WHERE DATE(last_play) >= DATE("now", "-7 days")')
            active_week = cursor.fetchone()[0]
            
            cursor.execute('SELECT SUM(total_games) FROM user_stats')
            total_games = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT SUM(total_questions) FROM user_stats')
            total_questions = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT SUM(total_correct) FROM user_stats')
            total_correct = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT SUM(total_time_played) FROM user_stats')
            total_time = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM game_history WHERE DATE(timestamp) = DATE("now")')
            games_today = cursor.fetchone()[0]
            
            avg_accuracy = (total_correct / total_questions * 100) if total_questions > 0 else 0
            
            stats_text = f"""
📊 **💎 Admin Dashboard 💎**

👥 **Users:**
• Total Users: {total_users}
• Active Today: {active_today}
• Active This Week: {active_week}
• Retention Rate: {(active_week/total_users*100) if total_users > 0 else 0:.1f}%

🎮 **Games:**
• Total Games: {total_games}
• Games Today: {games_today}
• Total Questions: {total_questions:,}
• Average Accuracy: {avg_accuracy:.1f}%
• Total Play Time: {total_time//3600}h {(total_time%3600)//60}m

📈 **Level Distribution:"""
            
            for level in range(1, 5):
                cursor.execute(f'SELECT SUM(level{level}_plays) FROM user_stats')
                level_plays = cursor.fetchone()[0] or 0
                cursor.execute(f'SELECT AVG(best_score_level{level}) FROM user_stats WHERE level{level}_plays > 0')
                avg_score = cursor.fetchone()[0] or 0
                stats_text += f"\n• Level {level}: {level_plays} games (avg: {avg_score:.1f}/13)"
            
            cursor.execute('SELECT SUM(easy_games), SUM(hard_games) FROM user_stats')
            mode_data = cursor.fetchone()
            easy_games = mode_data[0] or 0
            hard_games = mode_data[1] or 0
            
            stats_text += f"""

🎯 **Mode Distribution:**
• Easy Mode: {easy_games} games ({(easy_games/(easy_games+hard_games)*100) if (easy_games+hard_games) > 0 else 0:.1f}%)
• Hard Mode: {hard_games} games ({(hard_games/(easy_games+hard_games)*100) if (easy_games+hard_games) > 0 else 0:.1f}%)

🏆 **Top Performers:**"""
            
            cursor.execute('''
                SELECT u.first_name, u.last_name, s.total_games, 
                       (s.total_correct * 100.0 / s.total_questions) as accuracy
                FROM users u JOIN user_stats s ON u.user_id = s.user_id 
                WHERE s.total_questions > 0
                ORDER BY accuracy DESC, s.total_games DESC LIMIT 5
            ''')
            
            top_users = cursor.fetchall()
            for i, user in enumerate(top_users, 1):
                name = f"{user[0] or 'Unknown'} {user[1] or ''}".strip()
                stats_text += f"\n{i}. {name}: {user[3]:.1f}% ({user[2]} games)"
            
            stats_text += f"""

📅 **Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            conn.close()
            await update.message.reply_text(stats_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error in admin_stats_command: {e}")
            await update.message.reply_text("❌ Error retrieving admin statistics.")

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler untuk semua callback dari inline keyboard - STABILIZED VERSION"""
        query = update.callback_query
        
        # Jawab callback query dengan aman
        callback_answered = await self.safe_answer_callback(query)
        
        try:
            user_id = query.from_user.id
            data = query.data
            
            logger.info(f"Processing callback: {data} from user {user_id}")
            
            if data.startswith("level_"):
                level = int(data.split("_")[1])
                await self.start_level(query, level)
            elif data.startswith("start_quiz_"):
                parts = data.split("_")
                if len(parts) >= 4:
                    level = int(parts[2])
                    mode = parts[3]
                    await self.start_quiz(query, level, mode)
                else:
                    logger.error(f"Invalid start_quiz data: {data}")
                    await self.send_error_message(query, "Invalid quiz data")
            elif data.startswith("ans_"):
                await self.handle_answer(query, data)
            elif data == "my_stats":
                await self.show_user_stats_callback(query)
            elif data == "help":
                await self.show_help_callback(query)
            elif data == "back_to_menu":
                await self.back_to_menu(query)
            else:
                logger.warning(f"Unknown callback data: {data}")
                if not callback_answered:
                    try:
                        await query.answer("Unknown command")
                    except:
                        pass
                
        except Exception as e:
            logger.error(f"Error in button_callback with data '{query.data}': {e}", exc_info=True)
            await self.send_error_message(query, "Processing error occurred")

    async def send_error_message(self, query, error_msg: str):
        """Send error message safely with fallback"""
        try:
            await self.safe_edit_message(query, f"❌ {error_msg}. Please try /start")
        except Exception as e:
            logger.error(f"Could not send error message: {e}")
            # Last resort - just answer the callback
            try:
                await query.answer(f"❌ {error_msg}")
            except Exception as e2:
                logger.error(f"Could not even answer callback: {e2}")

    async def start_level(self, query, level: int):
        """Menampilkan informasi level dan pilihan mode"""
        try:
            level_chars = LEVELS[level]
            level_romaji = [HIRAGANA_DATA[char] for char in level_chars]
            level_names = {1: "Basic", 2: "Easy", 3: "Medium", 4: "Advanced"}
            
            level_info = f"""
🎯 **Level {level} ({level_names[level]}) Hiragana Quiz**

📋 **Quiz Details:**
• Questions: 13
• Time: 3 minutes (180 seconds)
• Characters: {len(level_chars)} hiragana
• Romaji sounds: {', '.join(level_romaji[:6])}...

🎮 **Select Mode:**
• **😊 Easy Mode**: Multiple choice (choose correct hiragana)
• **😤 Hard Mode**: True/False (more challenging!)

🖼️ **New Feature:**
Each question shows ROMAJI illustration, you choose the correct HIRAGANA character!

💡 **Tips:** 
Questions randomly selected from level characters. Best scores saved!

Choose your mode:
            """
            
            keyboard = [
                [InlineKeyboardButton("😊 Easy Mode", callback_data=f"start_quiz_{level}_easy")],
                [InlineKeyboardButton("😤 Hard Mode", callback_data=f"start_quiz_{level}_hard")],
                [InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_message(query, level_info, reply_markup)
            
        except Exception as e:
            logger.error(f"Error in start_level: {e}")
            await self.send_error_message(query, "Error loading level")

    async def start_quiz(self, query, level: int, mode: str):
        """Memulai quiz untuk level dan mode tertentu - STABILIZED VERSION"""
        user_id = query.from_user.id
        
        try:
            logger.info(f"Starting quiz - User: {user_id}, Level: {level}, Mode: {mode}")
            
            # Clear any existing session
            if user_id in quiz_sessions:
                del quiz_sessions[user_id]
            if user_id in used_romaji:
                del used_romaji[user_id]
            
            # Reset used romaji untuk user ini
            used_romaji[user_id] = []
            
            # Generate soal acak tanpa duplikat
            available_hiragana = LEVELS[level].copy()
            quiz_questions = []
            
            for i in range(13):
                if not available_hiragana:
                    available_hiragana = LEVELS[level].copy()
                
                char = random.choice(available_hiragana)
                available_hiragana.remove(char)
                
                correct_romaji = HIRAGANA_DATA[char]
                used_romaji[user_id].append(correct_romaji)
                
                if mode == "easy":
                    # Correct answer adalah huruf hiragana yang sesuai dengan romaji
                    correct_hiragana = char
                    
                    # Pilihan wrong answers: hiragana lain dari level yang sama
                    wrong_hiragana = [h for h in LEVELS[level] if h != correct_hiragana]
                    num_wrong = min(3, len(wrong_hiragana))
                    
                    if num_wrong > 0:
                        selected_wrong = random.sample(wrong_hiragana, num_wrong)
                    else:
                        # Jika tidak ada cukup wrong answers dari level yang sama, ambil dari level lain
                        all_other_hiragana = [h for h in HIRAGANA_DATA.keys() if h != correct_hiragana]
                        selected_wrong = random.sample(all_other_hiragana, min(3, len(all_other_hiragana)))
                    
                    options_hiragana = selected_wrong + [correct_hiragana]
                    random.shuffle(options_hiragana)
                    
                    question_data = {
                        'type': 'multiple_choice',
                        'romaji': correct_romaji,
                        'correct_hiragana': correct_hiragana,
                        'options_hiragana': options_hiragana,
                        'correct_index': options_hiragana.index(correct_hiragana)
                    }
                else:
                    # Hard mode: True/False dengan romaji dan hiragana
                    all_hiragana = list(HIRAGANA_DATA.keys())
                    wrong_hiragana = [h for h in all_hiragana if h != char]
                    
                    if random.random() < 0.5:
                        displayed_hiragana = char
                        is_correct = True
                    else:
                        displayed_hiragana = random.choice(wrong_hiragana) if wrong_hiragana else char
                        is_correct = displayed_hiragana == char
                    
                    question_data = {
                        'type': 'true_false',
                        'romaji': correct_romaji,
                        'correct_hiragana': char,
                        'displayed_hiragana': displayed_hiragana,
                        'is_correct': is_correct
                    }
                
                quiz_questions.append(question_data)
            
            # Simpan session quiz
            quiz_sessions[user_id] = {
                'level': level,
                'mode': mode,
                'questions': quiz_questions,
                'current_question': 0,
                'score': 0,
                'start_time': time.time(),
                'time_limit': 180,
                'user_answers': []
            }
            
            logger.info(f"Quiz session created for user {user_id} with {len(quiz_questions)} questions")
            await self.show_question(query, user_id)
            
        except Exception as e:
            logger.error(f"Error in start_quiz: {e}", exc_info=True)
            await self.send_error_message(query, "Error starting quiz")

    async def show_question(self, query, user_id: int):
        """Menampilkan soal quiz dengan gambar romaji - STABILIZED"""
        if user_id not in quiz_sessions:
            await self.send_error_message(query, "Quiz session not found")
            return
        
        try:
            session = quiz_sessions[user_id]
            
            # Cek waktu
            elapsed_time = time.time() - session['start_time']
            if elapsed_time > session['time_limit']:
                await self.end_quiz(query, user_id, "⏰ Time's up!")
                return
            
            current_q = session['current_question']
            if current_q >= len(session['questions']):
                await self.end_quiz(query, user_id, "✅ Quiz completed!")
                return
            
            question = session['questions'][current_q]
            remaining_time = max(0, int(session['time_limit'] - elapsed_time))
            minutes = remaining_time // 60
            seconds = remaining_time % 60
            
            mode_emoji = "😊" if session['mode'] == "easy" else "😤"
            mode_text = "Easy Mode" if session['mode'] == "easy" else "Hard Mode"
            
            if question['type'] == 'multiple_choice':
                question_text = f"""
🎯 **Question {current_q + 1}/13** {mode_emoji} **{mode_text}**
⏰ Time Left: {minutes:02d}:{seconds:02d}
📊 Current Score: {session['score']}/{current_q}

**Which Hiragana character represents this sound?**

*Romaji sound: {question['romaji']}*

Choose the correct Hiragana character:
                """
                
                keyboard = []
                for i, hiragana_char in enumerate(question['options_hiragana']):
                    callback_data = f"ans_mc_{user_id}_{current_q}_{i}"
                    keyboard.append([InlineKeyboardButton(
                        f"{chr(65+i)}. {hiragana_char}", 
                        callback_data=callback_data
                    )])
            
            else:
                question_text = f"""
🎯 **Question {current_q + 1}/13** {mode_emoji} **{mode_text}**
⏰ Time Left: {minutes:02d}:{seconds:02d}
📊 Current Score: {session['score']}/{current_q}

**True or False?**
The Hiragana character **{question['displayed_hiragana']}** is read as **"{question['romaji']}"**

*Romaji sound: {question['romaji']}*

Choose your answer:
                """
                
                keyboard = [
                    [InlineKeyboardButton("✅ True", callback_data=f"ans_tf_{user_id}_{current_q}_true")],
                    [InlineKeyboardButton("❌ False", callback_data=f"ans_tf_{user_id}_{current_q}_false")]
                ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Delete previous message safely
            try:
                await query.message.delete()
            except Exception as e:
                logger.warning(f"Could not delete previous message: {e}")
            
            # Send new question
            await self.send_quiz_with_image(
                query.message.chat_id,
                question_text,
                question['romaji'],
                reply_markup
            )
            
        except Exception as e:
            logger.error(f"Error in show_question: {e}", exc_info=True)
            await self.send_error_message(query, "Error displaying question")

    async def handle_answer(self, query, callback_data: str):
        """Handle jawaban user - COMPLETELY STABILIZED VERSION"""
        try:
            logger.info(f"Processing answer: {callback_data}")
            
            # Parse callback data dengan validasi ketat
            parts = callback_data.split('_')
            if len(parts) < 5:
                logger.error(f"Invalid callback data format: {callback_data}")
                await query.answer("❌ Invalid answer format")
                return
            
            answer_type = parts[1]  # 'mc' atau 'tf'
            
            try:
                user_id_from_callback = int(parts[2])
                question_num = int(parts[3])
            except (ValueError, IndexError):
                logger.error(f"Invalid user_id or question_num in callback: {callback_data}")
                await query.answer("❌ Invalid data format")
                return
            
            user_id = query.from_user.id
            
            # Validasi user
            if user_id != user_id_from_callback:
                logger.warning(f"User ID mismatch: {user_id} vs {user_id_from_callback}")
                await query.answer("❌ Invalid user session")
                return
            
            # Check if session exists
            if user_id not in quiz_sessions:
                logger.warning(f"No quiz session found for user {user_id}")
                await query.answer("❌ Quiz session expired. Please /start")
                return
            
            session = quiz_sessions[user_id]
            current_q = session['current_question']
            
            # Validasi nomor soal
            if question_num != current_q:
                logger.warning(f"Question mismatch: expected {current_q}, got {question_num}")
                await query.answer("❌ Question expired")
                return
            
            # Check if quiz is finished
            if current_q >= len(session['questions']):
                await self.end_quiz(query, user_id, "✅ Quiz completed!")
                return
                
            question = session['questions'][current_q]
            
            # Process answer based on type
            if answer_type == 'mc':
                if len(parts) < 5:
                    logger.error(f"Invalid MC callback data: {callback_data}")
                    await query.answer("❌ Invalid multiple choice data")
                    return
                
                try:
                    option_index = int(parts[4])
                    
                    # Validate index range
                    if option_index < 0 or option_index >= len(question['options_hiragana']):
                        logger.error(f"Option index out of range: {option_index}")
                        await query.answer("❌ Invalid option selected")
                        return
                    
                    user_answer_char = question['options_hiragana'][option_index]
                    is_correct = user_answer_char == question['correct_hiragana']
                    user_answer = user_answer_char
                    
                    logger.info(f"MC Answer processed: {user_answer_char}, correct: {is_correct}")
                    
                except (ValueError, IndexError) as e:
                    logger.error(f"Error processing MC answer: {e}")
                    await query.answer("❌ Invalid choice")
                    return
                
            elif answer_type == 'tf':
                if len(parts) < 5:
                    logger.error(f"Invalid TF callback data: {callback_data}")
                    await query.answer("❌ Invalid true/false data")
                    return
                
                user_says_true = parts[4] == 'true'
                is_correct = user_says_true == question['is_correct']
                user_answer = "true" if user_says_true else "false"
                
                logger.info(f"TF Answer processed: {user_says_true}, correct: {is_correct}")
            
            else:
                logger.error(f"Unknown answer type: {answer_type}")
                await query.answer("❌ Unknown answer type")
                return
            
            # Record the answer
            session['user_answers'].append({
                'question': question,
                'user_answer': user_answer,
                'is_correct': is_correct,
                'timestamp': time.time()
            })
            
            # Update score
            if is_correct:
                session['score'] += 1
                result_emoji = "✅"
                result_text = "Correct!"
            else:
                result_emoji = "❌"
                if answer_type == 'mc':
                    result_text = f"Wrong! Correct character: **{question['correct_hiragana']}**"
                else:
                    correct_text = "True" if question['is_correct'] else "False"
                    result_text = f"Wrong! Correct answer: **{correct_text}**\\nRomaji **{question['romaji']}** = **{question['correct_hiragana']}**"
            
            # Move to next question
            session['current_question'] += 1
            
            # Prepare result message
            result_message = f"""
{result_emoji} **{result_text}**

📝 Romaji: **{question['romaji']}** = **{question['correct_hiragana']}**
📊 Score: {session['score']}/{current_q + 1}
📈 Progress: {((current_q + 1) / 13 * 100):.0f}%

{'Next question in 2 seconds...' if session['current_question'] < 13 else 'Completing quiz...'}
            """
            
            # Answer the callback query first
            try:
                await query.answer(f"{result_emoji} {result_text}")
            except Exception as e:
                logger.warning(f"Could not answer callback: {e}")
            
            # Send result message
            result_message_obj = None
            try:
                result_message_obj = await self.application.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=result_message,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Error sending result message: {e}")
            
            # Wait before next question
            await asyncio.sleep(2)
            
            # Clean up result message
            if result_message_obj:
                try:
                    await result_message_obj.delete()
                except Exception as e:
                    logger.warning(f"Could not delete result message: {e}")
            
            # Continue to next question or end quiz
            if session['current_question'] < 13:
                await self.show_question(query, user_id)
            else:
                await self.end_quiz(query, user_id, "✅ Quiz completed!")
            
        except Exception as e:
            logger.error(f"Error in handle_answer: {e}", exc_info=True)
            try:
                await query.answer("❌ Error processing answer")
            except:
                pass

    async def end_quiz(self, query, user_id: int, reason: str):
        """Mengakhiri quiz dengan laporan premium - STABILIZED"""
        if user_id not in quiz_sessions:
            logger.warning(f"End quiz called but no session for user {user_id}")
            return
        
        try:
            session = quiz_sessions[user_id]
            user_info = self.get_user_info(query.from_user)
            
            final_score = session['score']
            total_questions = session['current_question']
            percentage = (final_score / total_questions * 100) if total_questions > 0 else 0
            duration_seconds = int(time.time() - session['start_time'])
            duration_text = f"{duration_seconds // 60}m {duration_seconds % 60}s"
            
            # Determine grade
            if percentage >= 95:
                grade = "💎 Master"
                message = "Outstanding! You've achieved mastery level!"
            elif percentage >= 85:
                grade = "🏆 Excellent"
                message = "Excellent performance! You're almost there!"
            elif percentage >= 75:
                grade = "⭐ Great"
                message = "Great job! Consistent practice leads to mastery!"
            elif percentage >= 60:
                grade = "👍 Good"
                message = "Good effort! Keep practicing to improve!"
            else:
                grade = "💪 Practice Needed"
                message = "Don't give up! Every practice session counts!"
            
            # Get updated user stats
            current_stats = self.get_user_stats(user_id)
            
            # Prepare quiz result data
            quiz_result = {
                'level': session['level'],
                'mode': session['mode'],
                'score': final_score,
                'total': total_questions,
                'percentage': percentage,
                'duration': duration_text,
                'duration_seconds': duration_seconds,
                'grade': grade,
                'user_stats': current_stats,
                'questions': session['questions'],
                'answers': session['user_answers']
            }
            
            # Update database
            self.update_user_stats(user_id, quiz_result)
            
            # Send track record to owner
            await self.send_premium_track_record(user_info, quiz_result)
            
            # Prepare result text
            result_text = f"""
🎯 **{reason}**

💎 **Quiz Results**

👤 **Player:** {user_info['first_name']}
📊 **Level {session['level']} - {session['mode'].title()} Mode**

🎯 **Score:** {final_score}/{total_questions} ({percentage:.1f}%)
⏱️ **Time:** {duration_text}
🏆 **Grade:** {grade}

{message}

*What would you like to do next?*
            """
            
            keyboard = [
                [InlineKeyboardButton(f"🔄 Retry {session['mode'].title()}", callback_data=f"start_quiz_{session['level']}_{session['mode']}")],
                [InlineKeyboardButton(f"🎯 Try Different Mode", callback_data=f"level_{session['level']}")],
                [InlineKeyboardButton("📊 Detailed Stats", callback_data="my_stats")],
                [InlineKeyboardButton("💎 Main Menu", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Send final result
            try:
                await self.application.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=result_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Error sending quiz result: {e}")
                # Fallback - try to edit existing message
                try:
                    await self.safe_edit_message(query, result_text, reply_markup)
                except Exception as e2:
                    logger.error(f"Fallback edit also failed: {e2}")
            
            # Clean up session
            try:
                if user_id in quiz_sessions:
                    del quiz_sessions[user_id]
                if user_id in used_romaji:
                    del used_romaji[user_id]
                    
                logger.info(f"Quiz completed and cleaned up for user {user_id}: {final_score}/{total_questions}")
            except Exception as e:
                logger.error(f"Error cleaning up session: {e}")
            
        except Exception as e:
            logger.error(f"Error in end_quiz: {e}", exc_info=True)
            # Emergency cleanup
            try:
                if user_id in quiz_sessions:
                    del quiz_sessions[user_id]
                if user_id in used_romaji:
                    del used_romaji[user_id]
            except:
                pass
            
            try:
                await self.application.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Error completing quiz. Please try /start",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_menu")]])
                )
            except Exception as e2:
                logger.error(f"Could not send error message: {e2}")

    async def show_user_stats(self, chat_id: int, user_id: int):
        """Menampilkan statistik user"""
        try:
            stats = self.get_user_stats(user_id)
            
            overall_accuracy = (stats['total_correct'] / stats['total_questions'] * 100) if stats['total_questions'] > 0 else 0
            easy_acc = (stats['mode_stats']['easy']['correct'] / stats['mode_stats']['easy']['total'] * 100) if stats['mode_stats']['easy']['total'] > 0 else 0
            hard_acc = (stats['mode_stats']['hard']['correct'] / stats['mode_stats']['hard']['total'] * 100) if stats['mode_stats']['hard']['total'] > 0 else 0
            
            stats_text = f"""
📊 **💎 Your Statistics 💎**

🎮 **Overview:**
• Total Games: {stats['total_games']}
• Total Questions: {stats['total_questions']}
• Overall Accuracy: {overall_accuracy:.1f}%
• Total Play Time: {stats['total_time_played']//60}m {stats['total_time_played']%60}s

🏆 **Best Scores per Level:**
• Level 1: {stats['best_scores'][1]}/13
• Level 2: {stats['best_scores'][2]}/13  
• Level 3: {stats['best_scores'][3]}/13
• Level 4: {stats['best_scores'][4]}/13

📈 **Level Play Count:**
• Level 1: {stats['level_plays'][1]} games
• Level 2: {stats['level_plays'][2]} games
• Level 3: {stats['level_plays'][3]} games
• Level 4: {stats['level_plays'][4]} games

🎯 **Mode Performance:**
• Easy Mode: {stats['mode_stats']['easy']['games']} games, {easy_acc:.1f}% accuracy
• Hard Mode: {stats['mode_stats']['hard']['games']} games, {hard_acc:.1f}% accuracy

🔥 **Streaks:**
• Current Streak: {stats['current_streak']} perfect games
• Best Streak: {stats['best_streak']} perfect games

Keep practicing! 頑張って！ 💪
            """
            
            keyboard = [[InlineKeyboardButton("🏠 Back to Menu", callback_data="back_to_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=stats_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"Error in show_user_stats: {e}")
            await self.application.bot.send_message(chat_id, "❌ Error retrieving statistics.")

    async def show_user_stats_callback(self, query):
        """Menampilkan statistik user dari callback"""
        try:
            user_id = query.from_user.id
            stats = self.get_user_stats(user_id)
            
            overall_accuracy = (stats['total_correct'] / stats['total_questions'] * 100) if stats['total_questions'] > 0 else 0
            easy_acc = (stats['mode_stats']['easy']['correct'] / stats['mode_stats']['easy']['total'] * 100) if stats['mode_stats']['easy']['total'] > 0 else 0
            hard_acc = (stats['mode_stats']['hard']['correct'] / stats['mode_stats']['hard']['total'] * 100) if stats['mode_stats']['hard']['total'] > 0 else 0
            
            stats_text = f"""
📊 **💎 Your Statistics 💎**

🎮 **Overview:**
• Total Games: {stats['total_games']}
• Total Questions: {stats['total_questions']}
• Overall Accuracy: {overall_accuracy:.1f}%
• Total Play Time: {stats['total_time_played']//60}m {stats['total_time_played']%60}s

🏆 **Best Scores per Level:**
• Level 1: {stats['best_scores'][1]}/13
• Level 2: {stats['best_scores'][2]}/13
• Level 3: {stats['best_scores'][3]}/13
• Level 4: {stats['best_scores'][4]}/13

📈 **Level Play Count:**
• Level 1: {stats['level_plays'][1]} games
• Level 2: {stats['level_plays'][2]} games
• Level 3: {stats['level_plays'][3]} games
• Level 4: {stats['level_plays'][4]} games

🎯 **Mode Performance:**
• Easy Mode: {stats['mode_stats']['easy']['games']} games, {easy_acc:.1f}% accuracy
• Hard Mode: {stats['mode_stats']['hard']['games']} games, {hard_acc:.1f}% accuracy

🔥 **Streaks:**
• Current Streak: {stats['current_streak']} perfect games
• Best Streak: {stats['best_streak']} perfect games

Keep practicing! 頑張って！ 💪
            """
            
            keyboard = [[InlineKeyboardButton("🏠 Back to Menu", callback_data="back_to_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_message(query, stats_text, reply_markup)
            
        except Exception as e:
            logger.error(f"Error in show_user_stats_callback: {e}")
            await self.send_error_message(query, "Error retrieving statistics")

    async def show_help_callback(self, query):
        """Menampilkan bantuan dari callback"""
        help_text = """
💎 **Hiragana Master Pro - Guide** 💎

📚 **How to Play:**
1. Choose level (1-4) based on your skill
2. Select mode: Easy (multiple choice) or Hard (true/false)
3. Answer 13 questions in 3 minutes
4. Get your score and grade!

🎯 **Levels:**
• Level 1: Basic characters (あ-し)
• Level 2: Intermediate (す-ね)
• Level 3: Advanced (の-や)
• Level 4: Expert (ゆ-ぎ)

🏆 **Grading System:**
• 95-100%: 💎 Master
• 85-94%: 🏆 Excellent
• 75-84%: ⭐ Great
• 60-74%: 👍 Good
• <60%: 💪 Practice Needed

🖼️ **Feature:**
Each question shows ROMAJI sound, you choose the correct HIRAGANA character!

🤖 Happy learning! 頑張って！
        """
        
        keyboard = [[InlineKeyboardButton("🏠 Back to Menu", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_message(query, help_text, reply_markup)

    async def back_to_menu(self, query):
        """Kembali ke menu utama"""
        try:
            user_info = self.get_user_info(query.from_user)
            
            keyboard = [
                [
                    InlineKeyboardButton("🌱 Level 1 (Basic)", callback_data="level_1"),
                    InlineKeyboardButton("🌿 Level 2 (Easy)", callback_data="level_2")
                ],
                [
                    InlineKeyboardButton("🌳 Level 3 (Medium)", callback_data="level_3"),
                    InlineKeyboardButton("🌸 Level 4 (Advanced)", callback_data="level_4")
                ],
                [
                    InlineKeyboardButton("📊 My Statistics", callback_data="my_stats"),
                    InlineKeyboardButton("💡 How to Play", callback_data="help")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            welcome_text = f"""
💎 **Hiragana Master Pro** 💎

*Welcome back {user_info['first_name']}!* 👋

Ready to continue your Hiragana mastery journey?

Choose your level or check your progress!
            """
            
            await self.safe_edit_message(query, welcome_text, reply_markup)
            
        except Exception as e:
            logger.error(f"Error in back_to_menu: {e}")
            await self.send_error_message(query, "Error returning to menu")

    def run(self):
        """Menjalankan bot dengan enhanced logging"""
        print("="*60)
        print("🚀 Hiragana Master Pro Bot - Starting...")
        print("="*60)
        print(f"👑 Owner ID: {OWNER_ID}")
        print("🚀 Features: ACTIVE")
        print("📊 Database: SQLite3 with Advanced Analytics")
        print("🎯 Advanced Analytics: ENABLED")
        print("🛡️  Error Handling: STABILIZED")
        print("💎 Owner Commands: ENHANCED")
        print("💵 System Value: $15,000")
        print("="*60)
        print("Bot is ready! Press Ctrl+C to stop")
        print("="*60)
        
        try:
            self.application.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
        except KeyboardInterrupt:
            print("\n🛑 Bot stopped by user")
            print("👋 Goodbye!")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            print(f"❌ Fatal Error: {e}")

def main():
    """Jalankan bot dengan setup validation"""
    try:
        print("🔧 Initializing Hiragana Quiz Bot...")
        bot = PremiumHiraganaQuizBot()
        print("✅ Bot initialized successfully!")
        bot.run()
    except Exception as e:
        print(f"❌ Failed to initialize bot: {e}")
        logger.error(f"Bot initialization failed: {e}")

if __name__ == "__main__":
    main() 
