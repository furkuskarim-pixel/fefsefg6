#!/usr/bin/env python3
"""
NOTIFY PRO BOT - Premium версия
+ ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
- БЫСТРЫЙ ОТВЕТ
+ Кэширование данных
+ Асинхронные операции
+ Ускоренная обработка callback'ов
"""

import asyncio
import logging
import sqlite3
import re
import json
import csv
import io
import tempfile
import os
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
from functools import lru_cache
import time

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, ContentType,
    ReplyKeyboardRemove, FSInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    FloodWaitError,
    ChatAdminRequiredError,
    ChannelPrivateError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UserAlreadyParticipantError,
    UsernameNotOccupiedError,
    AuthKeyUnregisteredError
)

# ========== НАСТРОЙКА ЛОГИРОВАНИЯ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== КЭШИРОВАНИЕ ==========
class Cache:
    """Кэш для быстрого доступа к данным"""
    
    def __init__(self, ttl=60):
        self.cache = {}
        self.ttl = ttl
    
    def get(self, key):
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key, value):
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()

# ========== КОНФИГУРАЦИЯ ==========
class Config:
    BOT_TOKEN = "8594431132:AAFv42-oYlPIYieOL3LeW9dXcFsc3xtNNm8"
    DB_FILE = "notify_bot.db"
    DEFAULT_MAILING_INTERVAL = 60
    ADMIN_USERNAME = "vsegda_online"
    BOT_USERNAME = "Autosender"
    SUPPORT_LINK = "https://t.me/chikatilo110"
    CRYPTO_API_KEY = "530518:AANAFD1tjSIxZTx5c7jMxjsZKJ4nazci8oy"
    
    TRIAL_DAYS = 1
    TRIAL_MAX_ACCOUNTS = 2
    TRIAL_DAILY_MESSAGES = 20
    TRIAL_MAX_MAILINGS = 2
    TRIAL_MIN_INTERVAL = 200
    
    # ✅ НАСТРОЙКИ ПРОИЗВОДИТЕЛЬНОСТИ
    CACHE_TTL = 30  # секунд
    CALLBACK_TIMEOUT = 0.5  # таймаут для callback'ов

# ========== МОДЕЛИ ТАРИФОВ ==========
class Tariff:
    TRIAL = {
        "name": "🎁 Пробный",
        "price": "0₽/0₴",
        "price_num": 0,
        "accounts_limit": Config.TRIAL_MAX_ACCOUNTS,
        "mailings_limit": Config.TRIAL_MAX_MAILINGS,
        "min_interval": Config.TRIAL_MIN_INTERVAL,
        "daily_messages": Config.TRIAL_DAILY_MESSAGES,
        "features": [
            f"{Config.TRIAL_DAYS} день бесплатно",
            f"{Config.TRIAL_MAX_ACCOUNTS} аккаунта",
            f"{Config.TRIAL_MAX_MAILINGS} рассылки",
            f"До {Config.TRIAL_DAILY_MESSAGES} сообщений в день",
            "Минимальный интервал 3.5 мин"
        ]
    }
    
    BASIC = {
        "name": "💰 Базовый",
        "price": "240₴/400₽",
        "price_num": 6,
        "accounts_limit": 3,
        "mailings_limit": 5,
        "min_interval": 130,
        "daily_messages": 100,
        "features": [
            "3 аккаунта",
            "5 рассылок",
            "Интервал 2 мин",
            "Приоритетная поддержка",
            "Базовая статистика"
        ]
    }
    
    PRO = {
        "name": "💎 ПРО",
        "price": "390₴/650₽",
        "price_num": 9,
        "accounts_limit": 10,
        "mailings_limit": 15,
        "min_interval": 95,
        "daily_messages": 500,
        "features": [
            "10 аккаунтов",
            "15 рассылок",
            "Интервал 1.5 мин",
            "Высший приоритет",
            "Расширенная статистика",
            "Экспорт данных"
        ]
    }
    
    ULTIMATE = {
        "name": "👑 Ultimate",
        "price": "650₴/1100₽",
        "price_num": 15,
        "accounts_limit": 50,
        "mailings_limit": 50,
        "min_interval": 1,
        "daily_messages": 50000,
        "features": [
            "50 аккаунтов",
            "50 рассылок",
            "Любой интервал (от 1 сек)",
            "Личный менеджер",
            "Полная статистика",
            "Приоритет в очередях",
            "API доступ"
        ]
    }
    
    @classmethod
    def get(cls, name):
        return getattr(cls, name, None)

# ========== БАЗА ДАННЫХ С КЭШИРОВАНИЕМ ==========
class Database:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_FILE, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cache = Cache(ttl=Config.CACHE_TTL)
        self.create_tables()
        self.migrate()
        self.create_admin_mailing_table()
        self.migrate_mailings_media()
    
    def migrate_mailings_media(self):
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(mailings)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'content_type' not in columns:
            cursor.execute("ALTER TABLE mailings ADD COLUMN content_type TEXT DEFAULT 'text'")
        if 'file_id' not in columns:
            cursor.execute("ALTER TABLE mailings ADD COLUMN file_id TEXT")
        if 'caption' not in columns:
            cursor.execute("ALTER TABLE mailings ADD COLUMN caption TEXT")
        if 'media_group' not in columns:
            cursor.execute("ALTER TABLE mailings ADD COLUMN media_group TEXT")
        if 'message' not in columns:
            cursor.execute("ALTER TABLE mailings ADD COLUMN message TEXT")
        
        self.conn.commit()
        logger.info("✅ Таблица mailings обновлена: добавлены поля для медиа")
    
    def migrate(self):
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cursor.fetchall()]
        
        columns_to_add = {
            'tariff': "ALTER TABLE users ADD COLUMN tariff TEXT DEFAULT NULL",
            'is_banned': "ALTER TABLE users ADD COLUMN is_banned BOOLEAN DEFAULT 0",
            'subscription_until': "ALTER TABLE users ADD COLUMN subscription_until DATETIME",
            'trial_used': "ALTER TABLE users ADD COLUMN trial_used BOOLEAN DEFAULT 0",
            'trial_start': "ALTER TABLE users ADD COLUMN trial_start DATETIME",
            'trial_messages_sent': "ALTER TABLE users ADD COLUMN trial_messages_sent INTEGER DEFAULT 0",
            'trial_last_reset': "ALTER TABLE users ADD COLUMN trial_last_reset DATE",
            'daily_messages_sent': "ALTER TABLE users ADD COLUMN daily_messages_sent INTEGER DEFAULT 0",
            'messages_last_reset': "ALTER TABLE users ADD COLUMN messages_last_reset DATE"
        }
        
        for column, sql in columns_to_add.items():
            if column not in columns:
                cursor.execute(sql)
        
        cursor.execute("PRAGMA table_info(mailings)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'interval' not in columns:
            cursor.execute("ALTER TABLE mailings ADD COLUMN interval INTEGER DEFAULT 60")
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT,
                content TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
        logger.info("Миграция исходной базы данных завершена")
    
    def create_tables(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE,
                username TEXT,
                tariff TEXT DEFAULT NULL,
                is_banned BOOLEAN DEFAULT 0,
                subscription_until DATETIME,
                trial_used BOOLEAN DEFAULT 0,
                trial_start DATETIME,
                trial_messages_sent INTEGER DEFAULT 0,
                trial_last_reset DATE,
                daily_messages_sent INTEGER DEFAULT 0,
                messages_last_reset DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS telegram_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                phone TEXT,
                api_id TEXT,
                api_hash TEXT,
                session_string TEXT,
                is_active BOOLEAN DEFAULT 1,
                last_used DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mailings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                account_id INTEGER,
                name TEXT,
                target TEXT,
                interval INTEGER DEFAULT 60,
                is_active BOOLEAN DEFAULT 0,
                sent_count INTEGER DEFAULT 0,
                last_sent DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                tariff TEXT,
                amount REAL,
                invoice_id TEXT,
                status TEXT DEFAULT "pending",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                paid_at DATETIME
            )
        ''')
        
        self.conn.commit()
        logger.info("✅ Исходные таблицы созданы")
    
    def create_admin_mailing_table(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_mailings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text_content TEXT NOT NULL,
                send_time TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
        logger.info("✅ Таблица для массовых рассылок админа создана")
    
    # ========== МЕТОДЫ С КЭШИРОВАНИЕМ ==========
    
    def get_user(self, telegram_id: int):
        """Получить пользователя с кэшированием"""
        cache_key = f"user_{telegram_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cursor.fetchone()
        result = dict(row) if row else None
        
        if result:
            self.cache.set(cache_key, result)
        return result
    
    def get_account(self, account_id: int):
        """Получить аккаунт с кэшированием"""
        cache_key = f"account_{account_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM telegram_accounts WHERE id = ?", (account_id,))
        row = cursor.fetchone()
        result = dict(row) if row else None
        
        if result:
            self.cache.set(cache_key, result)
        return result
    
    def get_mailing(self, mailing_id: int):
        """Получить рассылку с кэшированием"""
        cache_key = f"mailing_{mailing_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM mailings WHERE id = ?", (mailing_id,))
        row = cursor.fetchone()
        result = dict(row) if row else None
        
        if result:
            self.cache.set(cache_key, result)
        return result
    
    def get_user_mailings(self, user_id: int):
        """Получить рассылки пользователя с кэшированием"""
        cache_key = f"user_mailings_{user_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM mailings WHERE user_id = ? ORDER BY id DESC", (user_id,))
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        
        self.cache.set(cache_key, result)
        return result
    
    def get_user_accounts(self, user_id: int):
        """Получить аккаунты пользователя с кэшированием"""
        cache_key = f"user_accounts_{user_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM telegram_accounts WHERE user_id = ? AND is_active = 1 ORDER BY id DESC", (user_id,))
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        
        self.cache.set(cache_key, result)
        return result
    
    def get_user_by_id(self, user_id: int):
        """Получить пользователя по ID с кэшированием"""
        cache_key = f"user_by_id_{user_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        result = dict(row) if row else None
        
        if result:
            self.cache.set(cache_key, result)
        return result
    
    def invalidate_user_cache(self, telegram_id: int, user_id: int = None):
        """Инвалидация кэша пользователя"""
        if telegram_id:
            self.cache.cache.pop(f"user_{telegram_id}", None)
        if user_id:
            self.cache.cache.pop(f"user_by_id_{user_id}", None)
            self.cache.cache.pop(f"user_mailings_{user_id}", None)
            self.cache.cache.pop(f"user_accounts_{user_id}", None)
    
    def invalidate_mailing_cache(self, mailing_id: int):
        """Инвалидация кэша рассылки"""
        self.cache.cache.pop(f"mailing_{mailing_id}", None)
    
    def invalidate_account_cache(self, account_id: int):
        """Инвалидация кэша аккаунта"""
        self.cache.cache.pop(f"account_{account_id}", None)
    
    # ========== МЕТОДЫ ДЛЯ МАССОВЫХ ТЕКСТОВЫХ РАССЫЛОК АДМИНА ==========
    
    def save_admin_mailing(self, text: str, send_time: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO admin_mailings (text_content, send_time)
            VALUES (?, ?)
        ''', (text, send_time))
        mailing_id = cursor.lastrowid
        self.conn.commit()
        return mailing_id
    
    def get_pending_admin_mailings(self):
        cursor = self.conn.cursor()
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        cursor.execute('''
            SELECT * FROM admin_mailings 
            WHERE status = 'pending' 
            AND (send_time = 'now' OR datetime(send_time) <= datetime(?))
            ORDER BY send_time
        ''', (now,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def mark_admin_mailing_sent(self, mailing_id: int):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE admin_mailings SET status = "sent" WHERE id = ?', (mailing_id,))
        self.conn.commit()
    
    # ========== МЕТОДЫ ДЛЯ ПОЛЬЗОВАТЕЛЬСКИХ РАССЫЛОК ==========
    
    def add_mailing(self, user_id: int, account_id: int, name: str, target: str, 
                    content_type: str = 'text', message_text: str = None,
                    file_id: str = None, caption: str = None, media_group: str = None,
                    interval: int = 60) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO mailings 
                (user_id, account_id, name, target, interval, content_type, 
                 message, file_id, caption, media_group)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id, account_id, name, target, interval,
                content_type, message_text, file_id, caption, media_group
            ))
            self.conn.commit()
            mailing_id = cursor.lastrowid
            self.invalidate_user_cache(None, user_id)
            return mailing_id
        except Exception as e:
            logger.error(f"Ошибка при добавлении рассылки: {e}")
            return None
    
    def update_mailing(self, mailing_id: int, **kwargs):
        cursor = self.conn.cursor()
        fields = []
        values = []
        
        for key, value in kwargs.items():
            fields.append(f"{key} = ?")
            values.append(value)
        
        values.append(mailing_id)
        query = f"UPDATE mailings SET {', '.join(fields)} WHERE id = ?"
        cursor.execute(query, values)
        self.conn.commit()
        
        self.invalidate_mailing_cache(mailing_id)
        # Инвалидируем кэш пользователя, если есть
        mailing = self.get_mailing(mailing_id)
        if mailing:
            self.invalidate_user_cache(None, mailing['user_id'])
        
        return True
    
    def update_mailing_stats(self, mailing_id: int, sent_count: int = 1):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE mailings SET sent_count = sent_count + ?, last_sent = ? WHERE id = ?",
            (sent_count, datetime.now(), mailing_id)
        )
        self.conn.commit()
        self.invalidate_mailing_cache(mailing_id)
    
    def set_mailing_active(self, mailing_id: int, is_active: bool = True):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE mailings SET is_active = ? WHERE id = ?",
            (1 if is_active else 0, mailing_id)
        )
        self.conn.commit()
        self.invalidate_mailing_cache(mailing_id)
    
    def get_active_mailings(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM mailings WHERE is_active = 1")
        return [dict(row) for row in cursor.fetchall()]
    
    def update_mailing_interval(self, mailing_id: int, interval: int):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE mailings SET interval = ? WHERE id = ?",
            (interval, mailing_id)
        )
        self.conn.commit()
        self.invalidate_mailing_cache(mailing_id)
    
    # ========== ПОЛЬЗОВАТЕЛИ ==========
    
    def add_user(self, telegram_id: int, username: str = None):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)",
                (telegram_id, username)
            )
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return False
    
    def start_trial_period(self, telegram_id: int):
        cursor = self.conn.cursor()
        trial_start = datetime.now()
        trial_end = trial_start + timedelta(days=Config.TRIAL_DAYS)
        
        cursor.execute(
            """UPDATE users SET 
               tariff = 'TRIAL',
               trial_used = 1,
               trial_start = ?,
               subscription_until = ?,
               trial_messages_sent = 0,
               trial_last_reset = ?,
               daily_messages_sent = 0,
               messages_last_reset = ?
               WHERE telegram_id = ?""",
            (trial_start, trial_end, trial_start.date(), trial_start.date(), telegram_id)
        )
        self.conn.commit()
        self.invalidate_user_cache(telegram_id, None)
    
    def update_user_tariff(self, telegram_id: int, tariff: str, days: int = 30):
        cursor = self.conn.cursor()
        subscription_until = datetime.now() + timedelta(days=days)
        cursor.execute(
            "UPDATE users SET tariff = ?, subscription_until = ? WHERE telegram_id = ?",
            (tariff, subscription_until, telegram_id)
        )
        self.conn.commit()
        self.invalidate_user_cache(telegram_id, None)
    
    def ban_user(self, telegram_id: int):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE users SET is_banned = 1 WHERE telegram_id = ?",
            (telegram_id,)
        )
        self.conn.commit()
        self.invalidate_user_cache(telegram_id, None)
    
    def unban_user(self, telegram_id: int):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE users SET is_banned = 0 WHERE telegram_id = ?",
            (telegram_id,)
        )
        self.conn.commit()
        self.invalidate_user_cache(telegram_id, None)
    
    def is_user_banned(self, telegram_id: int):
        user = self.get_user(telegram_id)
        return user and user.get('is_banned') == 1
    
    def has_subscription(self, telegram_id: int):
        user = self.get_user(telegram_id)
        if not user:
            return False
        if user.get('is_banned'):
            return False
        
        tariff = user.get('tariff')
        if not tariff:
            return False
        
        if tariff == "TRIAL":
            trial_start = user.get('trial_start')
            if trial_start:
                try:
                    trial_date = datetime.fromisoformat(trial_start) if isinstance(trial_start, str) else trial_start
                    trial_end = trial_date + timedelta(days=Config.TRIAL_DAYS)
                    if trial_end < datetime.now():
                        return False
                except:
                    return False
            return True
        
        subscription_until = user.get('subscription_until')
        if subscription_until:
            try:
                subscription_date = datetime.fromisoformat(subscription_until) if isinstance(subscription_until, str) else subscription_until
                if subscription_date < datetime.now():
                    return False
            except:
                return False
        return True
    
    def get_subscription_days_left(self, telegram_id: int):
        user = self.get_user(telegram_id)
        if not user:
            return 0
        
        tariff = user.get('tariff')
        
        if tariff == "TRIAL":
            trial_start = user.get('trial_start')
            if trial_start:
                try:
                    trial_date = datetime.fromisoformat(trial_start) if isinstance(trial_start, str) else trial_start
                    trial_end = trial_date + timedelta(days=Config.TRIAL_DAYS)
                    days_left = (trial_end - datetime.now()).days
                    return max(0, days_left)
                except:
                    return 0
        
        subscription_until = user.get('subscription_until')
        if subscription_until:
            try:
                subscription_date = datetime.fromisoformat(subscription_until) if isinstance(subscription_until, str) else subscription_until
                days_left = (subscription_date - datetime.now()).days
                return max(0, days_left)
            except:
                return 0
        
        return 0
    
    def can_send_message_today(self, telegram_id: int):
        user = self.get_user(telegram_id)
        if not user:
            return False
        
        tariff = user.get('tariff', 'TRIAL')
        
        if tariff == "TRIAL":
            daily_limit = Config.TRIAL_DAILY_MESSAGES
            sent_today = user.get('trial_messages_sent', 0)
            last_reset = user.get('trial_last_reset')
        else:
            if tariff == "BASIC":
                daily_limit = Tariff.BASIC['daily_messages']
            elif tariff == "PRO":
                daily_limit = Tariff.PRO['daily_messages']
            elif tariff == "ULTIMATE":
                daily_limit = Tariff.ULTIMATE['daily_messages']
            else:
                daily_limit = Config.TRIAL_DAILY_MESSAGES
            
            sent_today = user.get('daily_messages_sent', 0)
            last_reset = user.get('messages_last_reset')
        
        today = datetime.now().date()
        
        if last_reset:
            try:
                last_reset_date = datetime.fromisoformat(last_reset).date() if isinstance(last_reset, str) else last_reset
                if last_reset_date < today:
                    cursor = self.conn.cursor()
                    if tariff == "TRIAL":
                        cursor.execute(
                            "UPDATE users SET trial_messages_sent = 0, trial_last_reset = ? WHERE telegram_id = ?",
                            (today, telegram_id)
                        )
                    else:
                        cursor.execute(
                            "UPDATE users SET daily_messages_sent = 0, messages_last_reset = ? WHERE telegram_id = ?",
                            (today, telegram_id)
                        )
                    self.conn.commit()
                    self.invalidate_user_cache(telegram_id, None)
                    sent_today = 0
            except Exception as e:
                logger.error(f"Ошибка при проверке сброса счетчика: {e}")
        
        return sent_today < daily_limit
    
    def increment_message_count(self, telegram_id: int):
        user = self.get_user(telegram_id)
        if not user:
            return
        
        tariff = user.get('tariff', 'TRIAL')
        cursor = self.conn.cursor()
        
        if tariff == "TRIAL":
            cursor.execute(
                "UPDATE users SET trial_messages_sent = trial_messages_sent + 1 WHERE telegram_id = ?",
                (telegram_id,)
            )
        else:
            cursor.execute(
                "UPDATE users SET daily_messages_sent = daily_messages_sent + 1 WHERE telegram_id = ?",
                (telegram_id,)
            )
        
        self.conn.commit()
        self.invalidate_user_cache(telegram_id, None)
    
    def can_add_account(self, user_id: int):
        user = self.get_user_by_id(user_id)
        if not user:
            return False
        
        tariff = user.get('tariff', 'TRIAL')
        
        if tariff == "TRIAL":
            limit = Config.TRIAL_MAX_ACCOUNTS
        elif tariff == "BASIC":
            limit = Tariff.BASIC['accounts_limit']
        elif tariff == "PRO":
            limit = Tariff.PRO['accounts_limit']
        elif tariff == "ULTIMATE":
            limit = Tariff.ULTIMATE['accounts_limit']
        else:
            return False
        
        accounts = self.get_user_accounts(user_id)
        return len(accounts) < limit
    
    def can_add_mailing(self, user_id: int):
        user = self.get_user_by_id(user_id)
        if not user:
            return False
        
        tariff = user.get('tariff', 'TRIAL')
        
        if tariff == "TRIAL":
            limit = Config.TRIAL_MAX_MAILINGS
        elif tariff == "BASIC":
            limit = Tariff.BASIC['mailings_limit']
        elif tariff == "PRO":
            limit = Tariff.PRO['mailings_limit']
        elif tariff == "ULTIMATE":
            limit = Tariff.ULTIMATE['mailings_limit']
        else:
            return False
        
        mailings = self.get_user_mailings(user_id)
        return len(mailings) < limit
    
    def get_min_interval(self, user_id: int):
        user = self.get_user_by_id(user_id)
        if not user:
            return 3600
        
        tariff = user.get('tariff', 'TRIAL')
        
        if tariff == "TRIAL":
            return Config.TRIAL_MIN_INTERVAL
        elif tariff == "BASIC":
            return Tariff.BASIC['min_interval']
        elif tariff == "PRO":
            return Tariff.PRO['min_interval']
        elif tariff == "ULTIMATE":
            return 1
        else:
            return 3600
    
    # ========== АККАУНТЫ ==========
    
    def add_telegram_account(self, user_id: int, phone: str, api_id: str, api_hash: str, session_string: str = None):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO telegram_accounts (user_id, phone, api_id, api_hash, session_string, last_used) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, phone, api_id, api_hash, session_string, datetime.now())
            )
            self.conn.commit()
            account_id = cursor.lastrowid
            self.invalidate_user_cache(None, user_id)
            return account_id
        except Exception as e:
            logger.error(f"Error adding account: {e}")
            return None
    
    def update_account_session(self, account_id: int, session_string: str):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE telegram_accounts SET session_string = ?, last_used = ? WHERE id = ?",
            (session_string, datetime.now(), account_id)
        )
        self.conn.commit()
        self.invalidate_account_cache(account_id)
        # Инвалидируем кэш пользователя
        account = self.get_account(account_id)
        if account:
            self.invalidate_user_cache(None, account['user_id'])
    
    def get_all_accounts(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM telegram_accounts ORDER BY id DESC")
        return [dict(row) for row in cursor.fetchall()]
    
    def deactivate_account(self, account_id: int):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE telegram_accounts SET is_active = 0 WHERE id = ?",
            (account_id,)
        )
        self.conn.commit()
        self.invalidate_account_cache(account_id)
        # Инвалидируем кэш пользователя
        account = self.get_account(account_id)
        if account:
            self.invalidate_user_cache(None, account['user_id'])
    
    # ========== АДМИН ФУНКЦИИ ==========
    
    def get_all_users(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users ORDER BY id DESC")
        return [dict(row) for row in cursor.fetchall()]
    
    def get_banned_users(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE is_banned = 1 ORDER BY id DESC")
        return [dict(row) for row in cursor.fetchall()]
    
    def get_active_users_count(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_banned = 0")
        return cursor.fetchone()['count']
    
    def get_total_messages_sent(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(sent_count) as total FROM mailings")
        result = cursor.fetchone()
        return result['total'] or 0
    
    def clear_database(self):
        cursor = self.conn.cursor()
        tables = ['users', 'telegram_accounts', 'mailings', 'payments', 'templates', 'logs', 'admin_mailings']
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
        self.conn.commit()
        self.cache.clear()
        logger.info("База данных очищена")
    
    # ========== ШАБЛОНЫ ==========
    
    def add_template(self, user_id: int, name: str, content: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO templates (user_id, name, content) VALUES (?, ?, ?)",
                (user_id, name, content)
            )
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка при добавлении шаблона: {e}")
            return None
    
    def get_user_templates(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM templates WHERE user_id = ? ORDER BY id DESC", (user_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_template(self, template_id: int):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM templates WHERE id = ?", (template_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def delete_template(self, template_id: int):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM templates WHERE id = ?", (template_id,))
        self.conn.commit()
    
    def add_log(self, user_id: int, action: str, details: str = ""):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO logs (user_id, action, details) VALUES (?, ?, ?)",
            (user_id, action, details)
        )
        self.conn.commit()
    
    def add_payment(self, user_id: int, tariff: str, amount: float, invoice_id: str):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO payments (user_id, tariff, amount, invoice_id) VALUES (?, ?, ?, ?)",
            (user_id, tariff, amount, invoice_id)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def update_payment_status(self, invoice_id: str, status: str):
        cursor = self.conn.cursor()
        if status == 'paid':
            cursor.execute(
                "UPDATE payments SET status = ?, paid_at = ? WHERE invoice_id = ?",
                (status, datetime.now(), invoice_id)
            )
        else:
            cursor.execute(
                "UPDATE payments SET status = ? WHERE invoice_id = ?",
                (status, invoice_id)
            )
        self.conn.commit()

# ========== МЕНЕДЖЕР TELEGRAM АККАУНТОВ ==========
class TelegramAccountManager:
    def __init__(self):
        self.active_clients = {}
        self.client_cache = {}  # Кэш клиентов для быстрого доступа
    
    async def create_client(self, api_id: str, api_hash: str, session_string: str = None):
        try:
            session = StringSession(session_string) if session_string else StringSession()
            client = TelegramClient(
                session=session,
                api_id=int(api_id),
                api_hash=api_hash,
                connection_retries=3,
                timeout=30
            )
            await client.connect()
            new_session_string = client.session.save()
            
            return {
                "success": True,
                "client": client,
                "session_string": new_session_string
            }
        except Exception as e:
            logger.error(f"Error creating client: {e}")
            return {"success": False, "error": str(e)}
    
    async def send_code_request(self, client, phone: str):
        try:
            await client.send_code_request(phone)
            return {"success": True}
        except Exception as e:
            logger.error(f"Error sending code: {e}")
            return {"success": False, "error": str(e)}
    
    async def sign_in(self, client, phone: str, code: str):
        try:
            await client.sign_in(phone=phone, code=code)
            return {"success": True}
        except SessionPasswordNeededError:
            return {"success": False, "need_password": True}
        except PhoneCodeInvalidError:
            return {"success": False, "error": "Неверный код"}
        except Exception as e:
            logger.error(f"Error signing in: {e}")
            return {"success": False, "error": str(e)}
    
    async def sign_in_with_password(self, client, password: str):
        try:
            await client.sign_in(password=password)
            return {"success": True}
        except Exception as e:
            logger.error(f"Error signing in with password: {e}")
            return {"success": False, "error": str(e)}
    
    async def test_connection(self, client):
        try:
            me = await client.get_me()
            return {
                "success": True,
                "user_id": me.id,
                "username": me.username,
                "first_name": me.first_name,
                "phone": me.phone
            }
        except Exception as e:
            logger.error(f"Error testing connection: {e}")
            return {"success": False, "error": str(e)}
    
    async def send_message(self, client, target: str, message: str):
        """Отправка текстового сообщения с обработкой всех возможных ошибок"""
        try:
            entity = await client.get_entity(target)
            await client.send_message(entity, message)
            return {"success": True, "sent": 1}
        except AuthKeyUnregisteredError:
            return {"success": False, "error": "❌ Аккаунт не авторизован. Удалите его и добавьте заново."}
        except FloodWaitError as e:
            return {"success": False, "error": f"Flood wait: {e.seconds} сек"}
        except (InviteHashExpiredError, InviteHashInvalidError):
            return {"success": False, "error": "❌ Срок действия ссылки истёк или она недействительна"}
        except UsernameNotOccupiedError:
            return {"success": False, "error": "❌ Пользователь/канал с таким username не найден"}
        except ChannelPrivateError:
            return {"success": False, "error": "❌ Канал/чат приватный, у вас нет доступа"}
        except ChatAdminRequiredError:
            return {"success": False, "error": "❌ Требуются права администратора для отправки в этот чат"}
        except UserAlreadyParticipantError:
            return {"success": False, "error": "❌ Вы уже участник, но не можете отправить сообщение?"}
        except ValueError as e:
            if "Cannot parse" in str(e):
                return {"success": False, "error": "❌ Неверный формат ссылки"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.error(f"Error sending message: {type(e).__name__}: {e}")
            return {"success": False, "error": f"{type(e).__name__}: {str(e)[:100]}"}
    
    async def send_photo(self, client, target: str, photo_path: str, caption: str = None):
        try:
            entity = await client.get_entity(target)
            await client.send_file(entity, photo_path, caption=caption)
            return {"success": True, "sent": 1}
        except AuthKeyUnregisteredError:
            return {"success": False, "error": "❌ Аккаунт не авторизован. Удалите и добавьте заново."}
        except FloodWaitError as e:
            return {"success": False, "error": f"Flood wait: {e.seconds} сек"}
        except (InviteHashExpiredError, InviteHashInvalidError):
            return {"success": False, "error": "❌ Ссылка недействительна"}
        except Exception as e:
            logger.error(f"Error sending photo: {e}")
            return {"success": False, "error": str(e)}
    
    async def send_video(self, client, target: str, video_path: str, caption: str = None):
        try:
            entity = await client.get_entity(target)
            await client.send_file(entity, video_path, caption=caption)
            return {"success": True, "sent": 1}
        except AuthKeyUnregisteredError:
            return {"success": False, "error": "❌ Аккаунт не авторизован. Удалите и добавьте заново."}
        except FloodWaitError as e:
            return {"success": False, "error": f"Flood wait: {e.seconds} сек"}
        except (InviteHashExpiredError, InviteHashInvalidError):
            return {"success": False, "error": "❌ Ссылка недействительна"}
        except Exception as e:
            logger.error(f"Error sending video: {e}")
            return {"success": False, "error": str(e)}
    
    async def disconnect(self, client):
        try:
            await client.disconnect()
            return True
        except:
            return False

# ========== МЕНЕДЖЕР РАССЫЛОК С ОПТИМИЗАЦИЕЙ ==========
class MailingManager:
    def __init__(self, db: Database, account_manager: TelegramAccountManager):
        self.db = db
        self.account_manager = account_manager
        self.active_tasks = {}
        self.mailing_queue = asyncio.Queue()
        self.processing = False
    
    async def start_mailing(self, mailing_id: int, chat_id: int = None):
        mailing = self.db.get_mailing(mailing_id)
        if not mailing:
            logger.error(f"Рассылка {mailing_id} не найдена")
            return False
        
        if mailing_id in self.active_tasks and not self.active_tasks[mailing_id].done():
            if chat_id:
                await bot.send_message(chat_id, "⚠️ Рассылка уже запущена")
            return False
        
        user = self.db.get_user_by_id(mailing['user_id'])
        if not user or not self.db.has_subscription(user['telegram_id']):
            if chat_id:
                await bot.send_message(chat_id, "❌ У вас нет активной подписки!")
            return False
        
        if not self.db.can_send_message_today(user['telegram_id']):
            if chat_id:
                await bot.send_message(chat_id, "❌ Вы достигли дневного лимита сообщений!")
            return False
        
        self.db.set_mailing_active(mailing_id, True)
        task = asyncio.create_task(self._mailing_loop(mailing_id, chat_id))
        self.active_tasks[mailing_id] = task
        logger.info(f"Рассылка {mailing_id} запущена")
        
        # Отправляем первое сообщение сразу
        asyncio.create_task(self._send_single_message(mailing_id, chat_id))
        return True
    
    async def stop_mailing(self, mailing_id: int):
        if mailing_id in self.active_tasks:
            task = self.active_tasks[mailing_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self.active_tasks[mailing_id]
            self.db.set_mailing_active(mailing_id, False)
            logger.info(f"Рассылка {mailing_id} остановлена")
        return True
    
    async def _send_single_message(self, mailing_id: int, chat_id: int = None):
        """Быстрая отправка одного сообщения"""
        try:
            mailing = self.db.get_mailing(mailing_id)
            if not mailing or not mailing.get('is_active'):
                return
            
            user = self.db.get_user_by_id(mailing['user_id'])
            if not user or not self.db.can_send_message_today(user['telegram_id']):
                if chat_id:
                    await bot.send_message(chat_id, "❌ Дневной лимит сообщений исчерпан")
                return
            
            account = self.db.get_account(mailing['account_id'])
            if not account:
                logger.error(f"Аккаунт {mailing['account_id']} не найден")
                if chat_id:
                    await bot.send_message(chat_id, "❌ Аккаунт не найден. Удалите рассылку и создайте заново.")
                return
            
            result = await self._send_mailing_message(mailing, account)
            
            if result["success"]:
                self.db.update_mailing_stats(mailing_id)
                self.db.increment_message_count(user['telegram_id'])
                logger.info(f"✅ Сообщение отправлено в рассылке {mailing_id}")
                
                if chat_id:
                    try:
                        messages_left = self._get_messages_left_today(user['telegram_id'])
                        await bot.send_message(
                            chat_id, 
                            f"✅ Сообщение отправлено!\n"
                            f"Отправлено: {mailing['sent_count'] + 1}\n"
                            f"Следующее через {mailing['interval']} сек\n"
                            f"Осталось сообщений сегодня: {messages_left}"
                        )
                    except:
                        pass
            else:
                error_text = result.get('error', 'Неизвестная ошибка')
                logger.error(f"❌ Ошибка отправки: {error_text}")
                if chat_id:
                    await bot.send_message(chat_id, f"❌ Ошибка: {error_text}")
        
        except Exception as e:
            logger.error(f"Ошибка в _send_single_message: {e}")
    
    def _get_messages_left_today(self, telegram_id: int):
        user = self.db.get_user(telegram_id)
        if not user:
            return 0
        
        tariff = user.get('tariff', 'TRIAL')
        
        if tariff == "TRIAL":
            daily_limit = Config.TRIAL_DAILY_MESSAGES
            sent_today = user.get('trial_messages_sent', 0)
        elif tariff == "BASIC":
            daily_limit = Tariff.BASIC['daily_messages']
            sent_today = user.get('daily_messages_sent', 0)
        elif tariff == "PRO":
            daily_limit = Tariff.PRO['daily_messages']
            sent_today = user.get('daily_messages_sent', 0)
        elif tariff == "ULTIMATE":
            daily_limit = Tariff.ULTIMATE['daily_messages']
            sent_today = user.get('daily_messages_sent', 0)
        else:
            return 0
        
        return max(0, daily_limit - sent_today)
    
    async def _mailing_loop(self, mailing_id: int, chat_id: int = None):
        """Оптимизированный цикл рассылки"""
        logger.info(f"Запущен цикл рассылки #{mailing_id}")
        
        while True:
            try:
                if mailing_id not in self.active_tasks or self.active_tasks[mailing_id].cancelled():
                    logger.info(f"Рассылка {mailing_id} остановлена (задача удалена)")
                    break
                
                mailing = self.db.get_mailing(mailing_id)
                if not mailing or not mailing.get('is_active'):
                    logger.info(f"Рассылка {mailing_id} остановлена (is_active=False)")
                    break
                
                user = self.db.get_user_by_id(mailing['user_id'])
                if not user or not self.db.has_subscription(user['telegram_id']):
                    if chat_id:
                        await bot.send_message(chat_id, "❌ Подписка закончилась. Рассылка остановлена.")
                    await self.stop_mailing(mailing_id)
                    break
                
                if not self.db.can_send_message_today(user['telegram_id']):
                    logger.info(f"Рассылка {mailing_id} приостановлена (лимит сообщений)")
                    await asyncio.sleep(60)
                    continue
                
                # Ждем интервал
                await asyncio.sleep(mailing['interval'])
                
                # Отправляем сообщение
                account = self.db.get_account(mailing['account_id'])
                if not account:
                    logger.error(f"Аккаунт {mailing['account_id']} не найден")
                    break
                
                result = await self._send_mailing_message(mailing, account)
                
                if result["success"]:
                    self.db.update_mailing_stats(mailing_id)
                    self.db.increment_message_count(user['telegram_id'])
                    logger.info(f"Сообщение #{mailing['sent_count'] + 1} отправлено в рассылке {mailing_id}")
                else:
                    error = result.get('error', '')
                    if any(x in error for x in [
                        'Аккаунт не авторизован',
                        'недействительна',
                        'истёк',
                        'не найден',
                        'доступа',
                        'прав администратора'
                    ]):
                        if chat_id:
                            await bot.send_message(chat_id, f"⛔ Рассылка остановлена из-за ошибки: {error}")
                        await self.stop_mailing(mailing_id)
                        break
            
            except asyncio.CancelledError:
                logger.info(f"Рассылка {mailing_id} отменена")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле рассылки {mailing_id}: {e}")
                await asyncio.sleep(60)
    
    async def _send_mailing_message(self, mailing: dict, account: dict):
        """Отправка сообщения рассылки"""
        try:
            # Создаем клиент
            result = await self.account_manager.create_client(
                account['api_id'],
                account['api_hash'],
                account['session_string']
            )
            
            if not result["success"]:
                return {"success": False, "error": result.get("error")}
            
            client = result["client"]
            send_result = None
            
            content_type = mailing.get('content_type', 'text')
            
            if content_type == 'text':
                send_result = await self.account_manager.send_message(
                    client,
                    mailing['target'],
                    mailing['message']
                )
            elif content_type in ('photo', 'photo_text', 'video', 'video_text'):
                file_id = mailing['file_id']
                if not file_id:
                    return {"success": False, "error": "Отсутствует file_id"}
                
                # Быстрая загрузка и отправка
                ext = '.jpg' if 'photo' in content_type else '.mp4'
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, f"telegram_media_{mailing['id']}_{datetime.now().timestamp()}{ext}")
                
                try:
                    await bot.download(file_id, destination=temp_path)
                    
                    if content_type in ('photo', 'video'):
                        caption = mailing.get('caption')
                    else:
                        caption = mailing.get('message')
                    
                    if 'photo' in content_type:
                        send_result = await self.account_manager.send_photo(
                            client,
                            mailing['target'],
                            temp_path,
                            caption
                        )
                    else:
                        send_result = await self.account_manager.send_video(
                            client,
                            mailing['target'],
                            temp_path,
                            caption
                        )
                except Exception as e:
                    logger.error(f"Ошибка обработки медиафайла: {e}")
                    return {"success": False, "error": f"Ошибка обработки файла: {e}"}
                finally:
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except:
                        pass
            else:
                send_result = {"success": False, "error": "Неизвестный тип контента"}
            
            # Обновляем сессию если нужно
            if result.get("session_string"):
                self.db.update_account_session(account['id'], result["session_string"])
            
            await self.account_manager.disconnect(client)
            return send_result
        
        except Exception as e:
            error_msg = f"Критическая ошибка: {type(e).__name__}: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    async def start_all_mailings(self):
        """Восстановление всех активных рассылок при запуске"""
        active_mailings = self.db.get_active_mailings()
        for mailing in active_mailings:
            mailing_id = mailing['id']
            if mailing_id not in self.active_tasks:
                await self.start_mailing(mailing_id)
                logger.info(f"Восстановлена рассылка {mailing_id}")

# ========== ПЛАТЕЖНАЯ СИСТЕМА ==========
class PaymentSystem:
    def __init__(self):
        self.pending_payments = {}
        self.api_key = Config.CRYPTO_API_KEY
        self.base_url = "https://pay.crypt.bot/api"
        self.payment_cache = Cache(ttl=300)  # Кэш платежей на 5 минут
    
    async def _request(self, method: str, endpoint: str, data: dict = None):
        """Отправка запроса к CryptoBot API"""
        headers = {
            "Crypto-Pay-API-Token": self.api_key,
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/{endpoint}"
            
            try:
                if method == "GET":
                    async with session.get(url, headers=headers, params=data) as response:
                        return await response.json()
                elif method == "POST":
                    async with session.post(url, headers=headers, json=data) as response:
                        return await response.json()
            except Exception as e:
                logger.error(f"Ошибка запроса к API: {e}")
                return None
    
    async def create_invoice(self, user_id: int, tariff: str) -> dict:
        """Создание счета в CryptoBot"""
        # Получаем данные тарифа
        if tariff == "TRIAL":
            tariff_data = Tariff.TRIAL
        elif tariff == "BASIC":
            tariff_data = Tariff.BASIC
        elif tariff == "PRO":
            tariff_data = Tariff.PRO
        elif tariff == "ULTIMATE":
            tariff_data = Tariff.ULTIMATE
        else:
            return None
        
        amount = tariff_data['price_num']
        if amount <= 0:
            return None
        
        try:
            data = {
                "asset": "USDT",
                "amount": str(amount),
                "payload": f"user_{user_id}_{tariff}",
                "description": f"Оплата тарифа {tariff_data['name']}"
            }
            
            result = await self._request("POST", "createInvoice", data)
            
            if result and result.get("ok"):
                invoice = result["result"]
                self.pending_payments[invoice["invoice_id"]] = {
                    'user_id': user_id,
                    'tariff': tariff,
                    'amount': amount,
                    'created_at': datetime.now()
                }
                self.payment_cache.set(invoice["invoice_id"], {
                    'status': 'pending',
                    'user_id': user_id,
                    'tariff': tariff
                })
                
                return {
                    'id': invoice["invoice_id"],
                    'url': invoice["pay_url"],
                    'amount': amount,
                    'asset': 'USDT',
                    'status': 'active'
                }
            else:
                error_msg = result.get('error', 'Неизвестная ошибка') if result else 'Нет ответа от API'
                logger.error(f"Ошибка API: {error_msg}")
                return None
        except Exception as e:
            logger.error(f"Ошибка создания счета: {e}")
            return None
    
    async def check_payment(self, invoice_id: str) -> bool:
        """Проверка статуса оплаты с кэшированием"""
        # Проверяем кэш
        cached = self.payment_cache.get(invoice_id)
        if cached and cached.get('status') == 'paid':
            return True
        
        try:
            result = await self._request("GET", "getInvoices", {
                "invoice_ids": invoice_id
            })
            
            if result and result.get("ok"):
                items = result.get("result", {}).get("items", [])
                if items:
                    status = items[0].get("status")
                    is_paid = status == "paid"
                    if is_paid:
                        self.payment_cache.set(invoice_id, {'status': 'paid'})
                    return is_paid
            
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки платежа: {e}")
            return False
    
    async def process_payment(self, invoice_id: str):
        """Обработка успешной оплаты"""
        if invoice_id in self.pending_payments:
            payment = self.pending_payments[invoice_id]
            user_id = payment['user_id']
            tariff = payment['tariff']
            
            # ТВОИ ЦИФРЫ
            if tariff == "BASIC":
                days = 165  # Базовый - 165 дней
            elif tariff == "PRO":
                days = 180  # ПРО - 180 дней
            elif tariff == "ULTIMATE":
                days = 360  # Ultimate - 360 дней
            else:
                days = 30  # На всякий случай
            
            # Выдаем подписку на нужное количество дней
            db.update_user_tariff(user_id, tariff, days)
            db.update_payment_status(invoice_id, 'paid')
            
            # Удаляем из ожидающих
            del self.pending_payments[invoice_id]
            
            # Обновляем кэш
            self.payment_cache.set(invoice_id, {'status': 'paid', 'user_id': user_id, 'tariff': tariff})
            
            logger.info(f"✅ Оплата {invoice_id} обработана. Пользователь {user_id}, тариф {tariff} на {days} дней")
            return user_id, tariff
        
        return None, None

# ========== СОСТОЯНИЯ ==========
class Form(StatesGroup):
    add_phone = State()
    add_api_id = State()
    add_api_hash = State()
    add_code = State()
    add_password = State()
    mailing_name = State()
    mailing_target = State()
    mailing_interval = State()
    mailing_account = State()
    template_name = State()
    template_content = State()
    admin_give_subscription = State()
    admin_ban_user = State()
    admin_unban_user = State()

class UserMailingStates(StatesGroup):
    choosing_type = State()
    waiting_name = State()
    waiting_media = State()
    waiting_caption = State()
    waiting_text = State()
    waiting_target = State()
    waiting_interval = State()
    waiting_account = State()

class EditMailingStates(StatesGroup):
    choosing_field = State()
    waiting_new_name = State()
    waiting_new_type = State()
    waiting_new_media = State()
    waiting_new_caption = State()
    waiting_new_text = State()
    waiting_new_target = State()
    waiting_new_interval = State()
    waiting_new_account = State()

class AdminBroadcastState(StatesGroup):
    waiting_text = State()
    waiting_time = State()

# ========== ИНИЦИАЛИЗАЦИЯ ==========
bot = Bot(token=Config.BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()
account_manager = TelegramAccountManager()
mailing_manager = MailingManager(db, account_manager)
payment_system = PaymentSystem()

# ========== КЛАВИАТУРЫ (КЭШИРОВАННЫЕ) ==========
_keyboard_cache = {}

def main_menu(user_tariff: str = None):
    cache_key = f"main_menu_{user_tariff}"
    if cache_key in _keyboard_cache:
        return _keyboard_cache[cache_key]
    
    builder = ReplyKeyboardBuilder()
    
    if user_tariff and user_tariff != "TRIAL":
        builder.button(text="📨 Мои рассылки")
        builder.button(text="➕ Новая рассылка")
        builder.button(text="👤 Мои аккаунты")
        builder.button(text="📊 Статистика")
        builder.button(text="💾 Шаблоны сообщений")
        builder.button(text="💎 Мой тариф")
        builder.button(text="❓ Помощь")
        builder.adjust(2, 2, 2, 1)
    else:
        builder.button(text="📨 Мои рассылки")
        builder.button(text="➕ Новая рассылка")
        builder.button(text="👤 Мои аккаунты")
        builder.button(text="💎 Тарифы")
        builder.button(text="❓ Помощь")
        builder.adjust(2, 2, 1)
    
    keyboard = builder.as_markup(resize_keyboard=True)
    _keyboard_cache[cache_key] = keyboard
    return keyboard

def admin_menu():
    cache_key = "admin_menu"
    if cache_key in _keyboard_cache:
        return _keyboard_cache[cache_key]
    
    builder = ReplyKeyboardBuilder()
    builder.button(text="📊 Статистика бота")
    builder.button(text="👥 Все пользователи")
    builder.button(text="🔴 Заблокированные")
    builder.button(text="📢 Текстовая рассылка всем")
    builder.button(text="🎁 Выдать подписку")
    builder.button(text="🔨 Забанить пользователя")
    builder.button(text="✅ Разбанить пользователя")
    builder.button(text="📱 Все аккаунты")
    builder.button(text="🧹 Очистить БД")
    builder.button(text="📤 Экспорт данных")
    builder.button(text="⬅️ Назад")
    builder.adjust(2, 2, 2, 2, 2, 1)
    
    keyboard = builder.as_markup(resize_keyboard=True)
    _keyboard_cache[cache_key] = keyboard
    return keyboard

def help_keyboard():
    cache_key = "help_keyboard"
    if cache_key in _keyboard_cache:
        return _keyboard_cache[cache_key]
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🆘 Поддержка", url=Config.SUPPORT_LINK)
    builder.button(text="💎 Тарифы", callback_data="show_tariffs")
    builder.button(text="📖 Как работает", callback_data="how_it_works")
    builder.adjust(1, 2)
    
    keyboard = builder.as_markup()
    _keyboard_cache[cache_key] = keyboard
    return keyboard

def tariffs_keyboard(show_trial: bool = True):
    cache_key = f"tariffs_keyboard_{show_trial}"
    if cache_key in _keyboard_cache:
        return _keyboard_cache[cache_key]
    
    builder = InlineKeyboardBuilder()
    
    if show_trial:
        builder.button(text="🎁 Пробный (1 день)", callback_data="tariff_TRIAL")
    builder.button(text="💰 Базовый (240₴/400₽)", callback_data="tariff_BASIC")
    builder.button(text="💎 ПРО (390₴/650₽)", callback_data="tariff_PRO")
    builder.button(text="👑 Ultimate (650₴/1100₽)", callback_data="tariff_ULTIMATE")
    builder.button(text="🆘 Поддержка", url=Config.SUPPORT_LINK)
    builder.adjust(1, 1, 1, 1, 1)
    
    keyboard = builder.as_markup()
    _keyboard_cache[cache_key] = keyboard
    return keyboard

def templates_keyboard(templates):
    """Динамическая клавиатура - не кэшируем"""
    builder = InlineKeyboardBuilder()
    for template in templates:
        builder.button(text=f"📝 {template['name']}", callback_data=f"template_{template['id']}")
    builder.button(text="➕ Новый шаблон", callback_data="new_template")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def mailing_control_keyboard(mailing_id: int, is_active: bool = False):
    """Динамическая клавиатура - не кэшируем"""
    builder = InlineKeyboardBuilder()
    
    if is_active:
        builder.button(text="⏸️ Приостановить", callback_data=f"pause_mailing_{mailing_id}")
    else:
        builder.button(text="▶️ Запустить", callback_data=f"start_mailing_{mailing_id}")
    
    builder.button(text="⚙️ Интервал", callback_data=f"interval_mailing_{mailing_id}")
    builder.button(text="📊 Статистика", callback_data=f"stats_mailing_{mailing_id}")
    builder.button(text="✏️ Изменить", callback_data=f"edit_mailing_{mailing_id}")
    builder.button(text="🗑️ Удалить", callback_data=f"delete_mailing_{mailing_id}")
    builder.adjust(2, 2, 2)
    
    return builder.as_markup()

def mailing_type_keyboard():
    cache_key = "mailing_type_keyboard"
    if cache_key in _keyboard_cache:
        return _keyboard_cache[cache_key]
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Только текст", callback_data="user_mailing_text")
    builder.button(text="📸 Фото + текст", callback_data="user_mailing_photo_text")
    builder.button(text="🎥 Видео + текст", callback_data="user_mailing_video_text")
    builder.button(text="📸 Только фото", callback_data="user_mailing_photo")
    builder.button(text="🎥 Только видео", callback_data="user_mailing_video")
    builder.adjust(1)
    
    keyboard = builder.as_markup()
    _keyboard_cache[cache_key] = keyboard
    return keyboard

def edit_mailing_keyboard(mailing_id: int):
    """Динамическая клавиатура - не кэшируем"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Название", callback_data=f"edit_mailing_name_{mailing_id}")
    builder.button(text="📄 Контент", callback_data=f"edit_mailing_content_{mailing_id}")
    builder.button(text="⏱ Интервал", callback_data=f"edit_mailing_interval_{mailing_id}")
    builder.button(text="🔗 Ссылка", callback_data=f"edit_mailing_target_{mailing_id}")
    builder.button(text="📱 Аккаунт", callback_data=f"edit_mailing_account_{mailing_id}")
    builder.button(text="❌ Отмена", callback_data=f"edit_mailing_cancel_{mailing_id}")
    builder.adjust(1)
    
    return builder.as_markup()

# ========== ОСНОВНЫЕ КОМАНДЫ (ОПТИМИЗИРОВАННЫЕ) ==========
@dp.message(Command("start"))
async def cmd_start(message: Message):
    # Быстрая проверка бана
    if db.is_user_banned(message.from_user.id):
        await message.answer("❌ Вы заблокированы в этом боте!")
        return
    
    # Добавляем пользователя если его нет
    user = db.get_user(message.from_user.id)
    if not user:
        db.add_user(message.from_user.id, message.from_user.username)
        user = db.get_user(message.from_user.id)
    
    has_subscription = db.has_subscription(message.from_user.id)
    user_tariff = user.get('tariff') if user else None
    
    if not has_subscription and not user_tariff:
        welcome = f"""🎉 Добро пожаловать в NOTIFY PRO BOT, {message.from_user.first_name}!
🤖 Мощный бот для автоматических рассылок в Telegram

🚀 Пробный период:
• {Config.TRIAL_DAYS} день бесплатно
• {Config.TRIAL_MAX_ACCOUNTS} аккаунта
• {Config.TRIAL_MAX_MAILINGS} одновременные рассылки
• До {Config.TRIAL_DAILY_MESSAGES} сообщений в день
• Минимальный интервал: 3.5 мин

🎁 Чтобы начать пробный период, нажмите кнопку ниже!"""
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🎁 Активировать пробный период", callback_data="activate_trial")
        builder.button(text="💎 Посмотреть тарифы", callback_data="show_tariffs")
        builder.adjust(1)
        
        await message.answer(welcome, reply_markup=builder.as_markup())
    
    elif user_tariff == "TRIAL":
        days_left = db.get_subscription_days_left(message.from_user.id)
        welcome = f"""🎉 С возвращением в NOTIFY PRO BOT, {message.from_user.first_name}!
🤖 Вы используете пробный период

📅 Осталось дней: {days_left}
📱 Доступно аккаунтов: {Config.TRIAL_MAX_ACCOUNTS}
📨 Доступно рассылок: {Config.TRIAL_MAX_MAILINGS}
📤 Сообщений сегодня: {user.get('trial_messages_sent', 0)}/{Config.TRIAL_DAILY_MESSAGES}
⏱ Минимальный интервал: 3.5 мин

👉 Выберите действие в меню ниже:"""
        
        await message.answer(welcome, reply_markup=main_menu(user_tariff))
    
    else:
        days_left = db.get_subscription_days_left(message.from_user.id)
        tariff_name = Tariff.BASIC['name'] if user_tariff == "BASIC" else \
                     Tariff.PRO['name'] if user_tariff == "PRO" else \
                     Tariff.ULTIMATE['name'] if user_tariff == "ULTIMATE" else "Неизвестно"
        
        welcome = f"""🎉 С возвращением в NOTIFY PRO BOT, {message.from_user.first_name}!
🤖 Мощный бот для автоматических рассылок в Telegram

💎 Ваш тариф: {tariff_name}
📅 Осталось дней: {days_left}

🚀 Доступные функции:
• Авто-рассылка сообщений
• Шаблоны сообщений
• Расширенная статистика
• Управление несколькими аккаунтами
• Гибкие настройки интервалов
• Редактирование рассылок

👉 Выберите действие в меню ниже:"""
        
        await message.answer(welcome, reply_markup=main_menu(user_tariff))

@dp.message(Command("addaccount"))
async def start_add_account(message: Message, state: FSMContext):
    user = db.get_user(message.from_user.id)
    if not user or not db.has_subscription(message.from_user.id):
        await message.answer("❌ У вас нет активной подписки!")
        return
    
    if not db.can_add_account(user['id']):
        tariff = user.get('tariff', 'TRIAL')
        if tariff == "TRIAL":
            limit = Config.TRIAL_MAX_ACCOUNTS
        elif tariff == "BASIC":
            limit = Tariff.BASIC['accounts_limit']
        elif tariff == "PRO":
            limit = Tariff.PRO['accounts_limit']
        elif tariff == "ULTIMATE":
            limit = Tariff.ULTIMATE['accounts_limit']
        else:
            limit = 0
        
        await message.answer(
            f"❌ Достигнут лимит аккаунтов для вашего тарифа!\n\n📱 Максимум: {limit} аккаунтов\n\n💎 Увеличьте тариф для добавления большего количества аккаунтов.",
            reply_markup=main_menu(user.get('tariff'))
        )
        return
    
    await message.answer(
        "Введите номер телефона аккаунта (с кодом страны):\nПример: +79991234567",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Form.add_phone)

@dp.message(Command("admin"))
async def admin_command(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        await message.answer("❌ Доступ запрещен")
        return
    
    await message.answer(
        "👑 АДМИН ПАНЕЛЬ\n\nВыберите действие:",
        reply_markup=admin_menu()
    )

# ========== ОБРАБОТКА ТЕКСТОВЫХ КНОПОК МЕНЮ ==========
@dp.message(F.text == "📨 Мои рассылки")
async def show_mailings(message: Message):
    user = db.get_user(message.from_user.id)
    if not user or not db.has_subscription(message.from_user.id):
        await message.answer("❌ У вас нет активной подписки!")
        return
    
    mailings = db.get_user_mailings(user['id'])
    
    if not mailings:
        await message.answer("📭 У вас пока нет рассылок")
        return
    
    for m in mailings[:10]:
        account = db.get_account(m['account_id'])
        status = "🟢" if m['is_active'] else "🔴"
        text = (f"{status} {m['name']}\n"
                f"📱 Аккаунт: {account['phone'] if account else '?'}\n"
                f"🔗 Ссылка: {m['target'][:30]}...\n"
                f"⏱ Интервал: {m['interval']} сек\n"
                f"📊 Отправлено: {m['sent_count']}\n"
                f"📦 Тип: {m.get('content_type', 'text')}")
        
        await message.answer(text, reply_markup=mailing_control_keyboard(m['id'], m['is_active']))
    
    if len(mailings) > 10:
        await message.answer(f"... и еще {len(mailings) - 10} рассылок")

@dp.message(F.text == "➕ Новая рассылка")
async def start_user_mailing(message: Message, state: FSMContext):
    user = db.get_user(message.from_user.id)
    if not user or not db.has_subscription(message.from_user.id):
        await message.answer("❌ У вас нет активной подписки!")
        return
    
    if not db.can_add_mailing(user['id']):
        tariff = user.get('tariff', 'TRIAL')
        if tariff == "TRIAL":
            limit = Config.TRIAL_MAX_MAILINGS
        elif tariff == "BASIC":
            limit = Tariff.BASIC['mailings_limit']
        elif tariff == "PRO":
            limit = Tariff.PRO['mailings_limit']
        elif tariff == "ULTIMATE":
            limit = Tariff.ULTIMATE['mailings_limit']
        else:
            limit = 0
        
        await message.answer(
            f"❌ Достигнут лимит рассылок для вашего тарифа!\n\n📨 Максимум: {limit} рассылок\n\n💎 Увеличьте тариф для создания большего количества рассылок.",
            reply_markup=main_menu(user.get('tariff'))
        )
        return
    
    accounts = db.get_user_accounts(user['id'])
    if not accounts:
        await message.answer("❌ Сначала добавьте аккаунт командой /addaccount")
        return
    
    await message.answer(
        "📨 СОЗДАНИЕ НОВОЙ РАССЫЛКИ\n\n"
        "Выберите тип контента для рассылки:",
        reply_markup=mailing_type_keyboard()
    )
    await state.set_state(UserMailingStates.choosing_type)

@dp.message(F.text == "👤 Мои аккаунты")
async def show_accounts_menu(message: Message):
    user = db.get_user(message.from_user.id)
    if not user or not db.has_subscription(message.from_user.id):
        await message.answer("❌ У вас нет активной подписки!")
        return
    
    accounts = db.get_user_accounts(user['id'])
    
    if not accounts:
        await message.answer(
            "У вас пока нет аккаунтов.\n\nНапишите /addaccount чтобы добавить аккаунт.",
            reply_markup=ReplyKeyboardRemove()
        )
    else:
        text = "📋 Ваши аккаунты:\n\n"
        for acc in accounts:
            text += f"📱 {acc['phone']}\n"
        
        tariff = user.get('tariff', 'TRIAL')
        if tariff == "TRIAL":
            limit = Config.TRIAL_MAX_ACCOUNTS
        elif tariff == "BASIC":
            limit = Tariff.BASIC['accounts_limit']
        elif tariff == "PRO":
            limit = Tariff.PRO['accounts_limit']
        elif tariff == "ULTIMATE":
            limit = Tariff.ULTIMATE['accounts_limit']
        else:
            limit = 0
        
        text += f"\n📊 Лимит: {len(accounts)}/{limit} аккаунтов"
        await message.answer(text)

@dp.message(F.text == "📊 Статистика")
async def user_stats(message: Message):
    user = db.get_user(message.from_user.id)
    if not user or not db.has_subscription(message.from_user.id):
        await message.answer("❌ Эта функция доступна только с подпиской!")
        return
    
    accounts = db.get_user_accounts(user['id'])
    mailings = db.get_user_mailings(user['id'])
    total_sent = sum(mailing['sent_count'] for mailing in mailings)
    active_mailings = sum(1 for mailing in mailings if mailing['is_active'])
    
    tariff = user.get('tariff', 'TRIAL')
    
    if tariff == "TRIAL":
        accounts_limit = Config.TRIAL_MAX_ACCOUNTS
        mailings_limit = Config.TRIAL_MAX_MAILINGS
        daily_messages = Config.TRIAL_DAILY_MESSAGES
        messages_sent = user.get('trial_messages_sent', 0)
    elif tariff == "BASIC":
        accounts_limit = Tariff.BASIC['accounts_limit']
        mailings_limit = Tariff.BASIC['mailings_limit']
        daily_messages = Tariff.BASIC['daily_messages']
        messages_sent = user.get('daily_messages_sent', 0)
    elif tariff == "PRO":
        accounts_limit = Tariff.PRO['accounts_limit']
        mailings_limit = Tariff.PRO['mailings_limit']
        daily_messages = Tariff.PRO['daily_messages']
        messages_sent = user.get('daily_messages_sent', 0)
    elif tariff == "ULTIMATE":
        accounts_limit = Tariff.ULTIMATE['accounts_limit']
        mailings_limit = Tariff.ULTIMATE['mailings_limit']
        daily_messages = Tariff.ULTIMATE['daily_messages']
        messages_sent = user.get('daily_messages_sent', 0)
    else:
        accounts_limit = 0
        mailings_limit = 0
        daily_messages = 0
        messages_sent = 0
    
    text = (f"📊 ВАША СТАТИСТИКА\n\n"
            f"👤 Пользователь: @{user.get('username', 'нет')}\n"
            f"💎 Тариф: {tariff}\n"
            f"📅 Подписка до: {user.get('subscription_until', 'Неизвестно')}\n\n"
            f"📱 Аккаунты: {len(accounts)}/{accounts_limit}\n"
            f"📨 Рассылки: {len(mailings)}/{mailings_limit}\n"
            f"🚀 Активные рассылки: {active_mailings}\n"
            f"📤 Всего отправлено: {total_sent}\n"
            f"📈 Сообщений сегодня: {messages_sent}/{daily_messages}")
    
    await message.answer(text)

@dp.message(F.text == "💎 Мой тариф")
async def my_tariff(message: Message):
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return
    
    tariff = user.get('tariff')
    days_left = db.get_subscription_days_left(message.from_user.id)
    
    if not tariff:
        text = "🎁 ВАШ ТАРИФ\n\nУ вас нет активной подписки.\n\nАктивируйте пробный период или выберите тариф:"
        await message.answer(text, reply_markup=tariffs_keyboard(show_trial=True))
        return
    
    if tariff == "TRIAL":
        text = (f"🎁 ВАШ ТАРИФ: ПРОБНЫЙ\n\n"
                f"⏱ Осталось дней: {days_left}\n"
                f"📱 Можно добавить аккаунтов: {Config.TRIAL_MAX_ACCOUNTS}\n"
                f"📨 Можно создать рассылок: {Config.TRIAL_MAX_MAILINGS}\n"
                f"📤 Сообщений в день: {Config.TRIAL_DAILY_MESSAGES}\n"
                f"⏱ Минимальный интервал: 3.5 мин\n\n"
                f"После окончания пробного периода выберите подходящий тариф:")
        await message.answer(text, reply_markup=tariffs_keyboard(show_trial=False))
    else:
        if tariff == "BASIC":
            tariff_info = Tariff.BASIC
        elif tariff == "PRO":
            tariff_info = Tariff.PRO
        elif tariff == "ULTIMATE":
            tariff_info = Tariff.ULTIMATE
        else:
            await message.answer("❌ Неизвестный тариф")
            return
        
        features_text = "\n".join([f"• {feature}" for feature in tariff_info['features']])
        text = (f"💎 ВАШ ТАРИФ: {tariff_info['name']}\n\n"
                f"⏱ Осталось дней: {days_left}\n\n"
                f"📋 Включено:\n{features_text}\n\n"
                f"💰 Стоимость продления: {tariff_info['price']}\n\n"
                f"📞 Для продления: напишите @chikatilo110")
        await message.answer(text)

@dp.message(F.text == "💎 Тарифы")
async def handle_tariffs_button(message: Message):
    user = db.get_user(message.from_user.id)
    current_tariff = user.get('tariff', 'Нет') if user else "Нет"
    
    text = f"""💎 ТАРИФЫ NOTIFY PRO BOT

📊 Ваш текущий тариф: {current_tariff}

Выберите тариф для подробной информации:"""
    
    await message.answer(text, reply_markup=tariffs_keyboard(show_trial=True))

@dp.message(F.text == "💾 Шаблоны сообщений")
async def templates_menu(message: Message):
    user = db.get_user(message.from_user.id)
    if not user or not db.has_subscription(message.from_user.id):
        await message.answer("❌ Эта функция доступна только с подпиской!")
        return
    
    templates = db.get_user_templates(user['id'])
    
    if not templates:
        text = "📝 ШАБЛОНЫ СООБЩЕНИЙ\n\nУ вас пока нет шаблонов.\nСоздайте первый шаблон:"
    else:
        text = "📝 ШАБЛОНЫ СООБЩЕНИЙ\n\nВаши шаблоны:\n"
        for template in templates[:10]:
            text += f"• {template['name']}\n"
        if len(templates) > 10:
            text += f"\n... и еще {len(templates) - 10} шаблонов"
    
    await message.answer(text, reply_markup=templates_keyboard(templates))

@dp.message(F.text == "❓ Помощь")
async def show_help(message: Message):
    help_text = """📚 ПОМОЩЬ ПО ИСПОЛЬЗОВАНИЮ БОТА

Как работает бот:

1️⃣ Добавление аккаунта
• Получите API ID и Hash на my.telegram.org
• Добавьте аккаунт через /addaccount
• Подтвердите вход кодом из SMS

2️⃣ Создание рассылки
• Нажмите "➕ Новая рассылка"
• Выберите тип контента (текст/фото/видео)
• Укажите ссылку на чат/канал
• Напишите текст сообщения (если нужно)
• Выберите аккаунт для отправки

3️⃣ Запуск рассылки
• Нажмите "Запустить" в списке рассылок
• Бот автоматически отправляет сообщения
• Интервал настраивается под ваш тариф

⚠️ Важные моменты:
• Используйте ссылки-приглашения
• Не спамьте, соблюдайте правила Telegram
• Соблюдайте дневные лимиты сообщений

Нужна помощь? Нажмите кнопку поддержки ниже!"""
    
    await message.answer(help_text, reply_markup=help_keyboard())

@dp.message(F.text == "⬅️ Назад")
async def back_to_main_menu(message: Message):
    user = db.get_user(message.from_user.id)
    
    if str(message.from_user.username) == Config.ADMIN_USERNAME:
        await admin_command(message)
    else:
        await cmd_start(message)

# ========== АДМИН ХЕНДЛЕРЫ ==========
@dp.message(F.text == "📊 Статистика бота")
async def admin_stats(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    try:
        users_count = len(db.get_all_users())
        active_users = db.get_active_users_count()
        banned_users = len(db.get_banned_users())
        total_accounts = len(db.get_all_accounts())
        total_mailings = len(db.get_active_mailings())
        total_messages = db.get_total_messages_sent()
        
        cursor = db.conn.cursor()
        cursor.execute("SELECT tariff, COUNT(*) as count FROM users WHERE tariff IS NOT NULL GROUP BY tariff")
        tariff_stats = cursor.fetchall()
        
        tariff_text = ""
        for row in tariff_stats:
            tariff_name = row['tariff']
            if tariff_name == "TRIAL":
                tariff_text += f"🎁 Пробный: {row['count']} чел.\n"
            elif tariff_name == "BASIC":
                tariff_text += f"💰 Базовый: {row['count']} чел.\n"
            elif tariff_name == "PRO":
                tariff_text += f"💎 ПРО: {row['count']} чел.\n"
            elif tariff_name == "ULTIMATE":
                tariff_text += f"👑 Ultimate: {row['count']} чел.\n"
        
        text = (f"📊 СТАТИСТИКА БОТА\n\n"
                f"👥 Пользователей всего: {users_count}\n"
                f"✅ Активных: {active_users}\n"
                f"🔴 Заблокированных: {banned_users}\n\n"
                f"📱 Аккаунтов всего: {total_accounts}\n"
                f"📨 Активных рассылок: {total_mailings}\n"
                f"📤 Всего сообщений: {total_messages}\n\n"
                f"💎 Распределение по тарифам:\n{tariff_text}")
        
        await message.answer(text)
    except Exception as e:
        logger.error(f"Ошибка в admin_stats: {e}")
        await message.answer(f"❌ Ошибка при получении статистики: {e}")

@dp.message(F.text == "👥 Все пользователи")
async def admin_all_users(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    users = db.get_all_users()
    if not users:
        await message.answer("📭 Нет пользователей")
        return
    
    text = "👥 ВСЕ ПОЛЬЗОВАТЕЛИ\n\n"
    for user in users[:50]:
        status = "🔴" if user['is_banned'] else "✅"
        tariff = user.get('tariff', 'Нет')
        text += f"{status} ID: {user['telegram_id']} | @{user['username'] or 'нет'} | Тариф: {tariff}\n"
    
    if len(users) > 50:
        text += f"\n... и еще {len(users) - 50} пользователей"
    
    await message.answer(text)

@dp.message(F.text == "🔴 Заблокированные")
async def admin_banned_users(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    banned_users = db.get_banned_users()
    if not banned_users:
        await message.answer("✅ Нет заблокированных пользователей")
        return
    
    text = "🔴 ЗАБЛОКИРОВАННЫЕ ПОЛЬЗОВАТЕЛИ\n\n"
    for user in banned_users[:50]:
        text += f"ID: {user['telegram_id']} | @{user['username'] or 'нет'}\n"
    
    if len(banned_users) > 50:
        text += f"\n... и еще {len(banned_users) - 50} пользователей"
    
    await message.answer(text)

@dp.message(F.text == "📢 Текстовая рассылка всем")
async def admin_broadcast_start(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    await message.answer(
        "📢 МАССОВАЯ ТЕКСТОВАЯ РАССЫЛКА\n\n"
        "Введите текст сообщения для рассылки всем пользователям:"
    )
    await state.set_state(AdminBroadcastState.waiting_text)

@dp.message(AdminBroadcastState.waiting_text)
async def admin_broadcast_text(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    await state.update_data(text=message.text)
    
    await message.answer(
        "⏰ Укажите время отправки:\n"
        "• сейчас\n"
        "• 14:30 (сегодня)\n"
        "• завтра 09:00\n"
        "• 2025-01-01 12:00"
    )
    await state.set_state(AdminBroadcastState.waiting_time)

@dp.message(AdminBroadcastState.waiting_time)
async def admin_broadcast_time(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    send_time = message.text.strip().lower()
    data = await state.get_data()
    text = data.get('text')
    
    mailing_id = db.save_admin_mailing(text, send_time)
    
    if send_time == "сейчас":
        await message.answer("⏳ Начинаю немедленную рассылку...")
        asyncio.create_task(send_admin_broadcast(bot, mailing_id, message))
        await message.answer("✅ Рассылка запущена в фоновом режиме!")
    else:
        await message.answer(
            f"✅ Текстовая рассылка запланирована!\n"
            f"ID: {mailing_id}\n"
            f"Время: {send_time}"
        )
    
    await state.clear()

@dp.message(F.text == "🎁 Выдать подписку")
async def admin_give_subscription_start(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    await message.answer(
        "🎁 ВЫДАТЬ ПОДПИСКУ ПОЛЬЗОВАТЕЛЮ\n\n"
        "Введите ID пользователя и тариф через пробел:\n"
        "Пример: 123456789 BASIC 30\n\n"
        "Тарифы: BASIC, PRO, ULTIMATE\n"
        "Число - количество дней (по умолчанию 30)"
    )
    await state.set_state(Form.admin_give_subscription)

@dp.message(Form.admin_give_subscription)
async def admin_give_subscription_process(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer("Ошибка: неверный формат\nИспользование: ID ТАРИФ [ДНИ]")
            return
        
        user_id = int(args[0])
        tariff = args[1].upper()
        days = int(args[2]) if len(args) > 2 else 30
        
        if tariff not in ["BASIC", "PRO", "ULTIMATE"]:
            await message.answer("Ошибка: неверный тариф. Используйте: BASIC, PRO, ULTIMATE")
            return
        
        db.update_user_tariff(user_id, tariff, days)
        
        asyncio.create_task(
            bot.send_message(
                chat_id=user_id,
                text=f"🎉 Вам выдана подписка!\n\nТариф: {tariff}\nСрок: {days} дней\n\nНовые возможности уже доступны!"
            )
        )
        
        await message.answer(f"✅ Пользователю {user_id} выдан тариф {tariff} на {days} дней")
    except ValueError:
        await message.answer("Ошибка: неверный формат ID или количества дней")
    except Exception as e:
        await message.answer(f"Ошибка: {str(e)}")
    
    await state.clear()

@dp.message(F.text == "🔨 Забанить пользователя")
async def admin_ban_user_start(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    await message.answer("🔨 ЗАБАНИТЬ ПОЛЬЗОВАТЕЛЯ\n\nВведите ID пользователя для блокировки:")
    await state.set_state(Form.admin_ban_user)

@dp.message(Form.admin_ban_user)
async def admin_ban_user_process(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    try:
        user_id = int(message.text)
        db.ban_user(user_id)
        
        user = db.get_user(user_id)
        if user:
            mailings = db.get_user_mailings(user['id'])
            for mailing in mailings:
                asyncio.create_task(mailing_manager.stop_mailing(mailing['id']))
        
        await message.answer(f"✅ Пользователь {user_id} заблокирован")
    except ValueError:
        await message.answer("Ошибка: неверный формат ID")
    except Exception as e:
        await message.answer(f"Ошибка: {str(e)}")
    
    await state.clear()

@dp.message(F.text == "✅ Разбанить пользователя")
async def admin_unban_user_start(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    await message.answer("✅ РАЗБАНИТЬ ПОЛЬЗОВАТЕЛЯ\n\nВведите ID пользователя для разблокировки:")
    await state.set_state(Form.admin_unban_user)

@dp.message(Form.admin_unban_user)
async def admin_unban_user_process(message: Message, state: FSMContext):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    try:
        user_id = int(message.text)
        db.unban_user(user_id)
        await message.answer(f"✅ Пользователь {user_id} разблокирован")
    except ValueError:
        await message.answer("Ошибка: неверный формат ID")
    except Exception as e:
        await message.answer(f"Ошибка: {str(e)}")
    
    await state.clear()

@dp.message(F.text == "📱 Все аккаунты")
async def admin_all_accounts(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    accounts = db.get_all_accounts()
    if not accounts:
        await message.answer("📭 Нет аккаунтов")
        return
    
    text = "📱 ВСЕ АККАУНТЫ\n\n"
    for acc in accounts[:30]:
        status = "✅" if acc['is_active'] else "🔴"
        user = db.get_user_by_id(acc['user_id'])
        username = f"@{user['username']}" if user and user.get('username') else f"ID: {user['telegram_id']}" if user else "Неизвестно"
        text += f"{status} {acc['phone']} | Владелец: {username}\n"
    
    if len(accounts) > 30:
        text += f"\n... и еще {len(accounts) - 30} аккаунтов"
    
    await message.answer(text)

@dp.message(F.text == "🧹 Очистить БД")
async def admin_clear_db(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, очистить", callback_data="clear_db_confirm")
    builder.button(text="❌ Нет, отмена", callback_data="clear_db_cancel")
    builder.adjust(2)
    
    await message.answer(
        "⚠️ ВНИМАНИЕ: ОЧИСТКА БАЗЫ ДАННЫХ\n\n"
        "Вы уверены, что хотите полностью очистить базу данных?\n"
        "Это действие нельзя отменить!\n\n"
        "Будут удалены:\n"
        "• Все пользователи\n"
        "• Все аккаунты\n"
        "• Все рассылки\n"
        "• Все платежи\n"
        "• Все логи",
        reply_markup=builder.as_markup()
    )

@dp.message(F.text == "📤 Экспорт данных")
async def admin_export_data(message: Message):
    if str(message.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    users = db.get_all_users()
    if not users:
        await message.answer("📭 Нет данных для экспорта")
        return
    
    await message.answer("⏳ Создаю файл экспорта...")
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Telegram ID', 'Username', 'Тариф', 'Заблокирован', 'Подписка до', 'Дата регистрации'])
    
    for user in users:
        writer.writerow([
            user['id'],
            user['telegram_id'],
            user.get('username', ''),
            user.get('tariff', ''),
            'Да' if user.get('is_banned') else 'Нет',
            user.get('subscription_until', ''),
            user.get('created_at', '')
        ])
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(output.getvalue())
        temp_path = f.name
    
    await message.answer_document(
        FSInputFile(temp_path, filename='users_export.csv'),
        caption="📤 Экспорт данных пользователей"
    )
    os.unlink(temp_path)

# ========== CALLBACK ХЕНДЛЕРЫ ==========
@dp.callback_query(F.data == "activate_trial")
async def activate_trial(callback: CallbackQuery):
    try:
        user = db.get_user(callback.from_user.id)
        if not user:
            await callback.answer("Ошибка: пользователь не найден", show_alert=True)
            return
        
        if user.get('trial_used'):
            await callback.answer("❌ Вы уже использовали пробный период!", show_alert=True)
            return
        
        db.start_trial_period(callback.from_user.id)
        
        await callback.message.edit_text(
            f"🎉 Пробный период активирован!\n\n"
            f"✅ Теперь у вас есть {Config.TRIAL_DAYS} день бесплатного использования!\n"
            f"📱 Можно добавить до {Config.TRIAL_MAX_ACCOUNTS} аккаунтов\n"
            f"📨 Можно создать до {Config.TRIAL_MAX_MAILINGS} рассылок\n"
            f"📤 До {Config.TRIAL_DAILY_MESSAGES} сообщений в день\n"
            f"⏱ Минимальный интервал 3.5 мин\n\n"
            f"Используйте меню ниже для начала работы!"
        )
        
        await callback.message.answer(
            "✅ Пробный период активирован! Теперь вы можете пользоваться всеми функциями бота.",
            reply_markup=main_menu("TRIAL")
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в activate_trial: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "show_tariffs")
async def show_tariffs_callback(callback: CallbackQuery):
    try:
        user = db.get_user(callback.from_user.id)
        current_tariff = user.get('tariff', 'Нет') if user else "Нет"
        
        text = f"""💎 ТАРИФЫ NOTIFY PRO BOT

📊 Ваш текущий тариф: {current_tariff}

Выберите тариф для подробной информации:"""
        
        await callback.message.edit_text(text, reply_markup=tariffs_keyboard(show_trial=True))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в show_tariffs_callback: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu_callback(callback: CallbackQuery):
    try:
        user = db.get_user(callback.from_user.id)
        user_tariff = user.get('tariff') if user else None
        
        if user_tariff == "TRIAL":
            days_left = db.get_subscription_days_left(callback.from_user.id)
            welcome = f"""🎉 С возвращением в NOTIFY PRO BOT, {callback.from_user.first_name}!
🤖 Вы используете пробный период

📅 Осталось дней: {days_left}
📱 Доступно аккаунтов: {Config.TRIAL_MAX_ACCOUNTS}
📨 Доступно рассылок: {Config.TRIAL_MAX_MAILINGS}
📤 Сообщений сегодня: {user.get('trial_messages_sent', 0)}/{Config.TRIAL_DAILY_MESSAGES}
⏱ Минимальный интервал: 3.5 мин

👉 Выберите действие в меню ниже:"""
        else:
            welcome = f"""🎉 С возвращением в NOTIFY PRO BOT, {callback.from_user.first_name}!
🤖 Мощный бот для автоматических рассылок в Telegram

👉 Выберите действие в меню ниже:"""
        
        await callback.message.edit_text(welcome, reply_markup=main_menu(user_tariff))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в back_to_main_menu_callback: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("tariff_"))
async def show_tariff_details(callback: CallbackQuery):
    try:
        tariff_name = callback.data.split("_")[1]
        
        if tariff_name == "TRIAL":
            tariff = Tariff.TRIAL
        elif tariff_name == "BASIC":
            tariff = Tariff.BASIC
        elif tariff_name == "PRO":
            tariff = Tariff.PRO
        elif tariff_name == "ULTIMATE":
            tariff = Tariff.ULTIMATE
        else:
            await callback.answer("Тариф не найден")
            return
        
        features_text = "\n".join([f"• {feature}" for feature in tariff['features']])
        
        if tariff_name == "TRIAL":
            text = f"""🎁 {tariff['name']}

⏱ Срок: {Config.TRIAL_DAYS} день

📋 Включено:
{features_text}

🎯 Как активировать:
Нажмите кнопку "Активировать пробный период"
После активации начнется отсчет {Config.TRIAL_DAYS} дня

📝 После окончания: выберите подходящий платный тариф"""
            
            builder = InlineKeyboardBuilder()
            builder.button(text="🎁 Активировать пробный период", callback_data="activate_trial")
            builder.button(text="⬅️ Назад", callback_data="show_tariffs")
            builder.adjust(1, 1)
        else:
            interval_desc = ""
            if tariff_name == "BASIC":
                interval_desc = "2 минуты"
            elif tariff_name == "PRO":
                interval_desc = "1.5 минуты"
            elif tariff_name == "ULTIMATE":
                interval_desc = "от 1 секунды"
            
            text = f"""💎 {tariff['name']}

💰 Стоимость: {tariff['price']}

📋 Включено:
{features_text}

⏱ Минимальный интервал: {interval_desc}

💡 Преимущества:
• Полная анонимность
• Приоритетная поддержка
• Автоматическое обновление
• Безопасное хранение данных

🛒 Как приобрести:
1. Напишите @chikatilo110
2. Укажите желаемый тариф
3. Получите реквизиты для оплаты
4. После оплаты тариф будет активирован

📞 Поддержка: @chikatilo110"""
            
            builder = InlineKeyboardBuilder()
            builder.button(text="🛒 Купить", callback_data=f"buy_{tariff_name}")
            builder.button(text="📞 Поддержка", url=Config.SUPPORT_LINK)
            builder.button(text="⬅️ Назад", callback_data="show_tariffs")
            builder.adjust(1, 1, 1)
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в show_tariff_details: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("buy_"))
async def buy_tariff(callback: CallbackQuery):
    tariff_name = callback.data.split("_")[1]
    
    if tariff_name == "TRIAL":
        await callback.answer("Это пробный тариф!")
        return
    
    user_id = callback.from_user.id
    
    # Получаем данные тарифа
    if tariff_name == "BASIC":
        tariff_data = Tariff.BASIC
        price_uah = "240₴"
        price_rub = "400₽"
    elif tariff_name == "PRO":
        tariff_data = Tariff.PRO
        price_uah = "390₴"
        price_rub = "650₽"
    elif tariff_name == "ULTIMATE":
        tariff_data = Tariff.ULTIMATE
        price_uah = "650₴"
        price_rub = "1100₽"
    else:
        await callback.answer("❌ Неизвестный тариф", show_alert=True)
        return
    
    # Быстро отвечаем, чтобы не было задержки
    await callback.answer("🔄 Создаю счет...")
    
    invoice = await payment_system.create_invoice(user_id, tariff_name)
    
    if not invoice:
        await callback.message.answer("❌ Ошибка создания счета", show_alert=True)
        return
    
    db.add_payment(user_id, tariff_name, invoice['amount'], invoice['id'])
    
    text = f"""💳 Оплата тарифа {tariff_data['name']}

💰 Стоимость: {price_uah} / {price_rub} ({invoice['amount']} USDT)

🔗 Ссылка для оплаты:
{invoice['url']}

⏳ После оплаты подписка активируется автоматически!"""
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_{invoice['id']}")
    builder.button(text="⬅️ Назад", callback_data=f"tariff_{tariff_name}")
    builder.adjust(1, 1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("check_"))
async def check_payment(callback: CallbackQuery):
    """Быстрая проверка статуса оплаты"""
    try:
        parts = callback.data.split("_")
        if len(parts) != 2:
            await callback.answer("❌ Неверный формат данных", show_alert=True)
            return
        
        invoice_id = parts[1]
        
        # Быстро отвечаем
        await callback.answer("🔄 Проверяю оплату...")
        
        if await payment_system.check_payment(invoice_id):
            user_id, tariff = await payment_system.process_payment(invoice_id)
            
            if user_id:
                # Получаем название тарифа
                if tariff == "BASIC":
                    tariff_name = Tariff.BASIC['name']
                elif tariff == "PRO":
                    tariff_name = Tariff.PRO['name']
                elif tariff == "ULTIMATE":
                    tariff_name = Tariff.ULTIMATE['name']
                else:
                    tariff_name = tariff
                
                await callback.message.edit_text(
                    f"✅ Оплата получена!\nТариф {tariff_name} активирован!"
                )
                
                # Уведомляем пользователя в фоне
                asyncio.create_task(
                    bot.send_message(
                        user_id,
                        f"🎉 Тариф {tariff_name} активирован!\nСпасибо за покупку!"
                    )
                )
            else:
                await callback.message.answer("❌ Ошибка активации тарифа")
        else:
            await callback.answer("❌ Оплата ещё не поступила", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка в check_payment: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "how_it_works")
async def show_how_it_works(callback: CallbackQuery):
    text = """🔧 Как работает бот:

1. Подготовка аккаунта
• Регистрируетесь на my.telegram.org
• Получаете API ID и Hash
• Добавляете в бота через /addaccount

2. Настройка рассылки
• Выбираете тип контента
• Указываете ссылку на чат/группу
• Пишете текст сообщения
• Выбираете интервал отправки

3. Запуск
• Бот авторизуется в вашем аккаунте
• Отправляет сообщения по расписанию
• Весь процесс автоматизирован

4. Мониторинг
• Видите статистику отправок
• Можете остановить/запустить в любой момент
• Все работает 24/7

📈 Преимущества:
• Безопасно (ваши данные защищены)
• Надежно (автовосстановление при сбоях)
• Удобно (управление через Telegram)"""
    
    await callback.message.edit_text(text, reply_markup=help_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "clear_db_confirm")
async def clear_db_confirm(callback: CallbackQuery):
    if str(callback.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    # Останавливаем все рассылки
    active_mailings = db.get_active_mailings()
    for mailing in active_mailings:
        await mailing_manager.stop_mailing(mailing['id'])
    
    db.clear_database()
    
    await callback.message.edit_text("✅ База данных полностью очищена!")
    await callback.answer()

@dp.callback_query(F.data == "clear_db_cancel")
async def clear_db_cancel(callback: CallbackQuery):
    if str(callback.from_user.username) != Config.ADMIN_USERNAME:
        return
    
    await callback.message.edit_text("❌ Очистка базы данных отменена")
    await callback.answer()

@dp.callback_query(F.data == "new_template")
async def new_template_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите название для нового шаблона:")
    await state.set_state(Form.template_name)
    await callback.answer()

@dp.callback_query(F.data.startswith("template_"))
async def use_template(callback: CallbackQuery):
    try:
        template_id = int(callback.data.split("_")[1])
        template = db.get_template(template_id)
        
        if not template:
            await callback.answer("❌ Шаблон не найден", show_alert=True)
            return
        
        user = db.get_user(callback.from_user.id)
        if not user or template['user_id'] != user['id']:
            await callback.answer("❌ Нет доступа к этому шаблону", show_alert=True)
            return
        
        await callback.message.edit_text(
            f"📝 Шаблон: {template['name']}\n\n{template['content']}\n\nСкопируйте текст и используйте в рассылке."
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в use_template: {e}")
        await callback.answer("❌ Ошибка при загрузке шаблона", show_alert=True)

# ========== ХЕНДЛЕРЫ ДЛЯ УПРАВЛЕНИЯ РАССЫЛКАМИ ==========
@dp.callback_query(F.data.startswith("interval_mailing_"))
async def change_mailing_interval(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_mailing_id=mailing_id)
    await callback.message.answer("Введите новый интервал в секундах:")
    await state.set_state(EditMailingStates.waiting_new_interval)
    await callback.answer()

@dp.message(EditMailingStates.waiting_new_interval)
async def process_edit_interval(message: Message, state: FSMContext):
    try:
        interval = int(message.text.strip())
        data = await state.get_data()
        mailing_id = data.get('edit_mailing_id')
        
        mailing = db.get_mailing(mailing_id)
        if not mailing:
            await message.answer("❌ Рассылка не найдена")
            await state.clear()
            return
        
        user = db.get_user(message.from_user.id)
        min_interval = db.get_min_interval(user['id'])
        
        if interval < min_interval:
            interval = min_interval
            await message.answer(f"⚠️ Минимальный интервал для вашего тарифа {min_interval} сек. Установлено: {interval} сек")
        
        if interval > 86400:
            await message.answer("❌ Интервал не может быть больше 24 часов (86400 секунд)")
            return
        
        db.update_mailing_interval(mailing_id, interval)
        await message.answer(f"✅ Интервал рассылки изменён на {interval} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число (интервал в секундах)")

@dp.callback_query(F.data.startswith("start_mailing_"))
async def start_mailing_handler(callback: CallbackQuery):
    try:
        mailing_id = int(callback.data.split("_")[-1])
        success = await mailing_manager.start_mailing(mailing_id, callback.message.chat.id)
        
        if success:
            await callback.answer("✅ Рассылка запущена!")
            mailing = db.get_mailing(mailing_id)
            if mailing:
                text = (f"🚀 Рассылка запущена!\n\n"
                        f"Название: {mailing['name']}\n"
                        f"Отправлено: {mailing['sent_count']} сообщений\n"
                        f"Интервал: {mailing['interval']} сек.\n\n"
                        f"✅ Сообщения отправляются автоматически!")
                await callback.message.edit_text(text, reply_markup=mailing_control_keyboard(mailing_id, True))
        else:
            await callback.answer("❌ Ошибка запуска рассылки", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка в start_mailing_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("pause_mailing_"))
async def pause_mailing_handler(callback: CallbackQuery):
    try:
        mailing_id = int(callback.data.split("_")[-1])
        await mailing_manager.stop_mailing(mailing_id)
        await callback.answer("⏸️ Рассылка приостановлена")
        
        mailing = db.get_mailing(mailing_id)
        if mailing:
            text = (f"⏸️ Рассылка приостановлена\n\n"
                    f"Название: {mailing['name']}\n"
                    f"Отправлено: {mailing['sent_count']} сообщений")
            await callback.message.edit_text(text, reply_markup=mailing_control_keyboard(mailing_id, False))
    except Exception as e:
        logger.error(f"Ошибка в pause_mailing_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("delete_mailing_"))
async def delete_mailing_handler(callback: CallbackQuery):
    try:
        mailing_id = int(callback.data.split("_")[-1])
        await mailing_manager.stop_mailing(mailing_id)
        
        cursor = db.conn.cursor()
        cursor.execute("DELETE FROM mailings WHERE id = ?", (mailing_id,))
        db.conn.commit()
        db.invalidate_mailing_cache(mailing_id)
        
        await callback.message.delete()
        await callback.answer("🗑️ Рассылка удалена")
    except Exception as e:
        logger.error(f"Ошибка в delete_mailing_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("stats_mailing_"))
async def stats_mailing_handler(callback: CallbackQuery):
    mailing_id = int(callback.data.split("_")[-1])
    mailing = db.get_mailing(mailing_id)
    
    if mailing:
        text = (f"📊 Статистика рассылки\n\n"
                f"Название: {mailing['name']}\n"
                f"Отправлено: {mailing['sent_count']} сообщений\n"
                f"Интервал: {mailing['interval']} сек\n"
                f"Статус: {'🟢 Активна' if mailing['is_active'] else '🔴 Остановлена'}\n"
                f"Тип: {mailing.get('content_type', 'text')}")
        await callback.answer(text, show_alert=True)
    else:
        await callback.answer("❌ Рассылка не найдена", show_alert=True)

@dp.callback_query(F.data.startswith("edit_mailing_"))
async def edit_mailing_start(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    mailing = db.get_mailing(mailing_id)
    
    if not mailing:
        await callback.answer("❌ Рассылка не найдена", show_alert=True)
        return
    
    user = db.get_user(callback.from_user.id)
    if mailing['user_id'] != user['id']:
        await callback.answer("❌ Это не ваша рассылка", show_alert=True)
        return
    
    await state.update_data(edit_mailing_id=mailing_id)
    text = f"✏️ Редактирование рассылки «{mailing['name']}»\n\nВыберите, что хотите изменить:"
    await callback.message.edit_text(text, reply_markup=edit_mailing_keyboard(mailing_id))
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_mailing_cancel_"))
async def edit_mailing_cancel(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    await state.clear()
    
    mailing = db.get_mailing(mailing_id)
    if mailing:
        account = db.get_account(mailing['account_id'])
        status = "🟢" if mailing['is_active'] else "🔴"
        text = (f"{status} {mailing['name']}\n"
                f"📱 Аккаунт: {account['phone'] if account else '?'}\n"
                f"🔗 Ссылка: {mailing['target'][:30]}...\n"
                f"⏱ Интервал: {mailing['interval']} сек\n"
                f"📊 Отправлено: {mailing['sent_count']}\n"
                f"📦 Тип: {mailing.get('content_type', 'text')}")
        await callback.message.edit_text(text, reply_markup=mailing_control_keyboard(mailing_id, mailing['is_active']))
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_mailing_name_"))
async def edit_mailing_name_start(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_mailing_id=mailing_id)
    await callback.message.answer("📝 Введите новое название рассылки:")
    await state.set_state(EditMailingStates.waiting_new_name)
    await callback.answer()

@dp.message(EditMailingStates.waiting_new_name)
async def process_edit_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 100:
        await message.answer("❌ Слишком длинное название (максимум 100 символов)")
        return
    
    data = await state.get_data()
    mailing_id = data.get('edit_mailing_id')
    
    db.update_mailing(mailing_id, name=name)
    await message.answer(f"✅ Название рассылки изменено на «{name}»")
    await state.clear()

@dp.callback_query(F.data.startswith("edit_mailing_target_"))
async def edit_mailing_target_start(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_mailing_id=mailing_id)
    await callback.message.answer("🔗 Введите новую ссылку на группу/канал/чат:")
    await state.set_state(EditMailingStates.waiting_new_target)
    await callback.answer()

@dp.message(EditMailingStates.waiting_new_target)
async def process_edit_target(message: Message, state: FSMContext):
    target = message.text.strip()
    data = await state.get_data()
    mailing_id = data.get('edit_mailing_id')
    
    db.update_mailing(mailing_id, target=target)
    await message.answer(f"✅ Ссылка изменена")
    await state.clear()

@dp.callback_query(F.data.startswith("edit_mailing_account_"))
async def edit_mailing_account_start(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    mailing = db.get_mailing(mailing_id)
    
    if not mailing:
        await callback.answer("❌ Рассылка не найдена", show_alert=True)
        return
    
    user = db.get_user_by_id(mailing['user_id'])
    accounts = db.get_user_accounts(user['id'])
    
    if not accounts:
        await callback.answer("❌ У вас нет аккаунтов", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for acc in accounts:
        builder.button(text=f"📱 {acc['phone']}", callback_data=f"edit_account_{mailing_id}_{acc['id']}")
    builder.adjust(1)
    
    await callback.message.edit_text("Выберите новый аккаунт для рассылки:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_account_"))
async def process_edit_account(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    mailing_id = int(parts[2])
    account_id = int(parts[3])
    
    db.update_mailing(mailing_id, account_id=account_id)
    await callback.message.edit_text("✅ Аккаунт изменён")
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_mailing_content_"))
async def edit_mailing_content_start(callback: CallbackQuery, state: FSMContext):
    mailing_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_mailing_id=mailing_id)
    await callback.message.answer(
        "📄 Изменение контента рассылки\n\n"
        "Выберите новый тип контента:",
        reply_markup=mailing_type_keyboard()
    )
    await state.set_state(EditMailingStates.waiting_new_type)
    await callback.answer()

@dp.callback_query(EditMailingStates.waiting_new_type, F.data.startswith("user_mailing_"))
async def process_edit_content_type(callback: CallbackQuery, state: FSMContext):
    action = callback.data.replace("user_mailing_", "")
    await state.update_data(content_type=action)
    
    if action == 'text':
        await callback.message.answer("✍️ Введите новый текст сообщения:")
        await state.set_state(EditMailingStates.waiting_new_text)
    elif action in ('photo', 'photo_text'):
        await callback.message.answer("📸 Отправьте новое фото для рассылки:")
        await state.set_state(EditMailingStates.waiting_new_media)
    elif action in ('video', 'video_text'):
        await callback.message.answer("🎥 Отправьте новое видео для рассылки:")
        await state.set_state(EditMailingStates.waiting_new_media)
    
    await callback.answer()

@dp.message(EditMailingStates.waiting_new_media, F.photo)
async def process_edit_photo(message: Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(file_id=file_id)
    
    data = await state.get_data()
    content_type = data.get('content_type')
    
    if content_type == 'photo':
        await message.answer("💬 Введите подпись к фото (или /skip для пропуска):")
        await state.set_state(EditMailingStates.waiting_new_caption)
    elif content_type == 'photo_text':
        await message.answer("✍️ Введите текст для отправки вместе с фото:")
        await state.set_state(EditMailingStates.waiting_new_text)

@dp.message(EditMailingStates.waiting_new_media, F.video)
async def process_edit_video(message: Message, state: FSMContext):
    file_id = message.video.file_id
    await state.update_data(file_id=file_id)
    
    data = await state.get_data()
    content_type = data.get('content_type')
    
    if content_type == 'video':
        await message.answer("💬 Введите подпись к видео (или /skip):")
        await state.set_state(EditMailingStates.waiting_new_caption)
    elif content_type == 'video_text':
        await message.answer("✍️ Введите текст для отправки вместе с видео:")
        await state.set_state(EditMailingStates.waiting_new_text)

@dp.message(EditMailingStates.waiting_new_caption)
async def process_edit_caption(message: Message, state: FSMContext):
    if message.text != "/skip":
        await state.update_data(caption=message.text)
    
    data = await state.get_data()
    mailing_id = data.get('edit_mailing_id')
    content_type = data.get('content_type')
    file_id = data.get('file_id')
    caption = data.get('caption')
    
    update_data = {
        'content_type': content_type,
        'file_id': file_id,
        'caption': caption,
        'message': None
    }
    
    db.update_mailing(mailing_id, **update_data)
    await message.answer("✅ Контент рассылки обновлён")
    await state.clear()

@dp.message(EditMailingStates.waiting_new_text)
async def process_edit_text(message: Message, state: FSMContext):
    text = message.text.strip()
    data = await state.get_data()
    mailing_id = data.get('edit_mailing_id')
    content_type = data.get('content_type')
    file_id = data.get('file_id')
    
    update_data = {
        'content_type': content_type,
        'message': text
    }
    if file_id:
        update_data['file_id'] = file_id
    
    db.update_mailing(mailing_id, **update_data)
    await message.answer("✅ Контент рассылки обновлён")
    await state.clear()

# ========== ХЕНДЛЕРЫ ДЛЯ СОЗДАНИЯ РАССЫЛКИ ==========
@dp.callback_query(UserMailingStates.choosing_type, F.data.startswith("user_mailing_"))
async def process_user_mailing_type(callback: CallbackQuery, state: FSMContext):
    action = callback.data.replace("user_mailing_", "")
    await state.update_data(content_type=action)
    await callback.message.edit_text("📝 Введите название рассылки (для вашего удобства):")
    await state.set_state(UserMailingStates.waiting_name)
    await callback.answer()

@dp.message(UserMailingStates.waiting_name)
async def process_user_mailing_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 100:
        await message.answer("❌ Слишком длинное название (максимум 100 символов)")
        return
    
    await state.update_data(name=name)
    data = await state.get_data()
    content_type = data.get('content_type')
    
    if content_type == 'text':
        await message.answer("✍️ Введите текст сообщения для рассылки:")
        await state.set_state(UserMailingStates.waiting_text)
    elif content_type in ('photo', 'photo_text'):
        await message.answer("📸 Отправьте фото для рассылки:")
        await state.set_state(UserMailingStates.waiting_media)
    elif content_type in ('video', 'video_text'):
        await message.answer("🎥 Отправьте видео для рассылки:")
        await state.set_state(UserMailingStates.waiting_media)

@dp.message(UserMailingStates.waiting_media, F.photo)
async def process_user_mailing_photo(message: Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(file_id=file_id)
    
    data = await state.get_data()
    content_type = data.get('content_type')
    
    if content_type == 'photo':
        await message.answer("💬 Добавьте подпись к фото (или /skip для пропуска):")
        await state.set_state(UserMailingStates.waiting_caption)
    elif content_type == 'photo_text':
        await message.answer("✍️ Теперь введите текст, который будет отправлен вместе с фото:")
        await state.set_state(UserMailingStates.waiting_text)

@dp.message(UserMailingStates.waiting_media, F.video)
async def process_user_mailing_video(message: Message, state: FSMContext):
    file_id = message.video.file_id
    await state.update_data(file_id=file_id)
    
    data = await state.get_data()
    content_type = data.get('content_type')
    
    if content_type == 'video':
        await message.answer("💬 Добавьте подпись к видео (или /skip):")
        await state.set_state(UserMailingStates.waiting_caption)
    elif content_type == 'video_text':
        await message.answer("✍️ Теперь введите текст, который будет отправлен вместе с видео:")
        await state.set_state(UserMailingStates.waiting_text)

@dp.message(UserMailingStates.waiting_caption)
async def process_user_mailing_caption(message: Message, state: FSMContext):
    if message.text != "/skip":
        await state.update_data(caption=message.text)
    
    await message.answer("🔗 Введите ссылку на группу/канал/чат для отправки:\nПример: https://t.me/+shGZsLRos7IwYjE6")
    await state.set_state(UserMailingStates.waiting_target)

@dp.message(UserMailingStates.waiting_text)
async def process_user_mailing_text(message: Message, state: FSMContext):
    await state.update_data(message_text=message.text)
    data = await state.get_data()
    content_type = data.get('content_type')
    
    if content_type in ('text', 'photo_text', 'video_text'):
        await message.answer("🔗 Введите ссылку на группу/канал/чат для отправки:")
        await state.set_state(UserMailingStates.waiting_target)

@dp.message(UserMailingStates.waiting_target)
async def process_user_mailing_target(message: Message, state: FSMContext):
    target = message.text.strip()
    await state.update_data(target=target)
    
    user = db.get_user(message.from_user.id)
    min_interval = db.get_min_interval(user['id'])
    
    await message.answer(
        f"⏱ Введите интервал отправки (в секундах):\n"
        f"⚠️ Минимум для вашего тарифа: {min_interval} сек\n"
        f"📊 Рекомендуем: {min_interval * 2} сек для безопасности"
    )
    await state.set_state(UserMailingStates.waiting_interval)

@dp.message(UserMailingStates.waiting_interval)
async def process_user_mailing_interval(message: Message, state: FSMContext):
    try:
        interval = int(message.text.strip())
        user = db.get_user(message.from_user.id)
        min_interval = db.get_min_interval(user['id'])
        
        if interval < min_interval:
            interval = min_interval
            await message.answer(f"⚠️ Минимальный интервал для вашего тарифа {min_interval} сек. Установлено: {interval} сек")
        
        if interval > 86400:
            await message.answer("❌ Интервал не может быть больше 24 часов (86400 секунд)")
            return
        
        await state.update_data(interval=interval)
        
        user = db.get_user(message.from_user.id)
        accounts = db.get_user_accounts(user['id'])
        
        builder = InlineKeyboardBuilder()
        for acc in accounts:
            builder.button(text=f"📱 {acc['phone']}", callback_data=f"select_acc_mailing_{acc['id']}")
        builder.adjust(1)
        
        await message.answer(
            f"✅ Интервал установлен: {interval} сек\n\n"
            "Выберите аккаунт для отправки:",
            reply_markup=builder.as_markup()
        )
        await state.set_state(UserMailingStates.waiting_account)
    except ValueError:
        await message.answer("❌ Введите число (интервал в секундах)")

@dp.callback_query(F.data.startswith("select_acc_mailing_"))
async def process_user_mailing_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.replace("select_acc_mailing_", ""))
    data = await state.get_data()
    
    user = db.get_user(callback.from_user.id)
    
    mailing_id = db.add_mailing(
        user_id=user['id'],
        account_id=account_id,
        name=data['name'],
        target=data['target'],
        content_type=data['content_type'],
        message_text=data.get('message_text'),
        file_id=data.get('file_id'),
        caption=data.get('caption'),
        interval=data.get('interval', 60)
    )
    
    if not mailing_id:
        await callback.answer("❌ Ошибка создания рассылки", show_alert=True)
        await state.clear()
        return
    
    await state.clear()
    
    account = db.get_account(account_id)
    type_names = {
        'text': '📝 Текст',
        'photo': '📸 Фото',
        'video': '🎥 Видео',
        'photo_text': '📸 Фото + Текст',
        'video_text': '🎥 Видео + Текст'
    }
    
    text = (f"✅ Рассылка создана!\n\n"
            f"📋 Название: {data['name']}\n"
            f"📱 Аккаунт: {account['phone'] if account else '?'}\n"
            f"🔗 Ссылка: {data['target'][:50]}...\n"
            f"⏱ Интервал: {data.get('interval', 60)} сек\n"
            f"📦 Тип: {type_names.get(data['content_type'], 'Неизвестно')}\n\n"
            f"Нажмите Запустить 👇")
    
    await callback.message.edit_text(text, reply_markup=mailing_control_keyboard(mailing_id, False))
    await callback.answer()

# ========== ХЕНДЛЕРЫ ДЛЯ ДОБАВЛЕНИЯ АККАУНТА ==========
@dp.message(Form.add_phone)
async def process_add_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await message.answer("❌ Неверный формат номера. Пример: +79991234567")
        return
    
    await state.update_data(phone=phone)
    await message.answer("Введите API ID (цифры с my.telegram.org):")
    await state.set_state(Form.add_api_id)

@dp.message(Form.add_api_id)
async def process_add_api_id(message: Message, state: FSMContext):
    api_id = message.text.strip()
    if not api_id.isdigit():
        await message.answer("❌ API ID должен содержать только цифры")
        return
    
    await state.update_data(api_id=api_id)
    await message.answer("Введите API Hash (строка с my.telegram.org):")
    await state.set_state(Form.add_api_hash)

@dp.message(Form.add_api_hash)
async def process_add_api_hash(message: Message, state: FSMContext):
    api_hash = message.text.strip()
    if len(api_hash) < 10:
        await message.answer("❌ API Hash слишком короткий")
        return
    
    await state.update_data(api_hash=api_hash)
    
    data = await state.get_data()
    phone = data['phone']
    api_id = data['api_id']
    
    result = await account_manager.create_client(api_id, api_hash)
    
    if not result["success"]:
        await message.answer(f"❌ Ошибка создания клиента: {result['error']}")
        await state.clear()
        return
    
    client = result["client"]
    code_result = await account_manager.send_code_request(client, phone)
    
    if not code_result["success"]:
        await message.answer(f"❌ Ошибка запроса кода: {code_result.get('error', 'Неизвестная ошибка')}")
        await client.disconnect()
        await state.clear()
        return
    
    await state.update_data(
        client=client,
        temp_session=result["session_string"]
    )
    
    await message.answer("Код отправлен! Введите код из SMS или Telegram (5 цифр):")
    await state.set_state(Form.add_code)

@dp.message(Form.add_code)
async def process_add_code(message: Message, state: FSMContext):
    code = message.text.strip()
    if not code.isdigit() or len(code) != 5:
        await message.answer("❌ Код должен быть 5 цифр. Пример: 12345")
        return
    
    data = await state.get_data()
    client = data.get('client')
    phone = data.get('phone')
    
    if not client:
        await message.answer("❌ Ошибка: клиент не найден.")
        await state.clear()
        return
    
    sign_in_result = await account_manager.sign_in(client, phone, code)
    
    if sign_in_result.get("need_password"):
        await message.answer("Введите пароль двухфакторной аутентификации:")
        await state.set_state(Form.add_password)
        return
    
    if not sign_in_result["success"]:
        await message.answer(f"❌ Ошибка входа: {sign_in_result.get('error', 'Неизвестная ошибка')}")
        await client.disconnect()
        await state.clear()
        return
    
    test_result = await account_manager.test_connection(client)
    
    if not test_result["success"]:
        await message.answer(f"❌ Ошибка проверки: {test_result['error']}")
        await client.disconnect()
        await state.clear()
        return
    
    session_string = client.session.save()
    
    user = db.get_user(message.from_user.id)
    account_id = db.add_telegram_account(
        user_id=user['id'],
        phone=phone,
        api_id=data['api_id'],
        api_hash=data['api_hash'],
        session_string=session_string
    )
    
    if account_id:
        db.update_account_session(account_id, session_string)
        await client.disconnect()
        await state.clear()
        
        await message.answer(
            f"✅ Аккаунт успешно добавлен!\n\nТелефон: {phone}\nИмя: {test_result.get('first_name', 'Неизвестно')}\n\nТеперь можно создать рассылку!",
            reply_markup=main_menu(user.get('tariff'))
        )

@dp.message(Form.add_password)
async def process_add_password(message: Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    client = data.get('client')
    
    if not client:
        await message.answer("❌ Ошибка: клиент не найден.")
        await state.clear()
        return
    
    password_result = await account_manager.sign_in_with_password(client, password)
    
    if not password_result["success"]:
        await message.answer(f"❌ Ошибка входа: {password_result.get('error', 'Неверный пароль')}")
        await client.disconnect()
        await state.clear()
        return
    
    test_result = await account_manager.test_connection(client)
    
    if not test_result["success"]:
        await message.answer(f"❌ Ошибка проверки: {test_result['error']}")
        await client.disconnect()
        await state.clear()
        return
    
    session_string = client.session.save()
    
    user = db.get_user(message.from_user.id)
    account_id = db.add_telegram_account(
        user_id=user['id'],
        phone=data['phone'],
        api_id=data['api_id'],
        api_hash=data['api_hash'],
        session_string=session_string
    )
    
    if account_id:
        db.update_account_session(account_id, session_string)
        await client.disconnect()
        await state.clear()
        
        await message.answer(
            f"✅ Аккаунт успешно добавлен (2FA)!\n\nТеперь можно создать рассылку!",
            reply_markup=main_menu(user.get('tariff'))
        )

# ========== ХЕНДЛЕРЫ ДЛЯ ШАБЛОНОВ ==========
@dp.message(Form.template_name)
async def process_template_name(message: Message, state: FSMContext):
    if len(message.text) > 50:
        await message.answer("❌ Слишком длинное название (максимум 50 символов)")
        return
    
    await state.update_data(template_name=message.text)
    await message.answer("Введите текст шаблона:")
    await state.set_state(Form.template_content)

@dp.message(Form.template_content)
async def process_template_content(message: Message, state: FSMContext):
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ошибка: пользователь не найден")
        await state.clear()
        return
    
    data = await state.get_data()
    template_name = data['template_name']
    
    template_id = db.add_template(user['id'], template_name, message.text)
    
    if template_id:
        await message.answer(f"✅ Шаблон '{template_name}' успешно создан!")
    else:
        await message.answer("❌ Ошибка при создании шаблона")
    
    await state.clear()

# ========== ОБРАБОТКА НЕИЗВЕСТНЫХ КОМАНД ==========
@dp.callback_query()
async def handle_unknown_callback(callback: CallbackQuery):
    logger.warning(f"Неизвестный callback: {callback.data} от пользователя {callback.from_user.id}")
    await callback.answer("❌ Неизвестная команда")

@dp.message()
async def handle_unknown_message(message: Message):
    logger.warning(f"Неизвестное сообщение: {message.text} от пользователя {message.from_user.id}")
    await message.answer("❌ Неизвестная команда. Используйте меню или команду /start")

# ========== ФУНКЦИИ ДЛЯ ФОНОВЫХ ЗАДАЧ ==========
async def send_admin_broadcast(bot: Bot, mailing_id: int, status_message: Message = None):
    """Отправка массовой рассылки в фоне"""
    conn = sqlite3.connect(Config.DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT telegram_id FROM users WHERE is_banned = 0")
    users = cursor.fetchall()
    
    cursor.execute("SELECT text_content FROM admin_mailings WHERE id = ?", (mailing_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return 0
    
    text = row[0]
    success = 0
    total = len(users)
    
    for i, (user_id,) in enumerate(users):
        try:
            await bot.send_message(user_id, text, parse_mode='HTML')
            success += 1
            
            # Отправляем прогресс каждые 10 сообщений
            if i % 10 == 0 and status_message and i > 0:
                try:
                    await status_message.edit_text(f"⏳ Прогресс: {i}/{total} отправлено...")
                except:
                    pass
            
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
            continue
    
    db.mark_admin_mailing_sent(mailing_id)
    
    if status_message:
        try:
            await status_message.edit_text(f"✅ Рассылка завершена! Отправлено: {success}/{total}")
        except:
            pass
    
    return success

async def admin_mailing_scheduler():
    """Планировщик административных рассылок"""
    while True:
        try:
            pending = db.get_pending_admin_mailings()
            for mailing in pending:
                logger.info(f"Отправка массовой рассылки #{mailing['id']}")
                asyncio.create_task(send_admin_broadcast(bot, mailing['id']))
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Ошибка в планировщике: {e}")
            await asyncio.sleep(60)

async def payment_checker():
    """Планировщик проверки платежей"""
    while True:
        try:
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT * FROM payments WHERE status = 'pending' AND created_at > datetime('now', '-1 day')"
            )
            pending = cursor.fetchall()
            
            for payment in pending:
                payment = dict(payment)
                if await payment_system.check_payment(payment['invoice_id']):
                    user_id, tariff = await payment_system.process_payment(payment['invoice_id'])
                    if user_id:
                        logger.info(f"Оплата {payment['invoice_id']} обработана")
                        try:
                            await bot.send_message(
                                user_id,
                                f"🎉 Оплата получена!\nТариф {tariff} активирован!"
                            )
                        except:
                            pass
            
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Ошибка проверки платежей: {e}")
            await asyncio.sleep(60)

# ========== ЗАПУСК БОТА ==========
async def on_startup():
    """Действия при запуске бота"""
    logger.info("=" * 60)
    logger.info("🤖 NOTIFY PRO BOT - Premium версия")
    logger.info("=" * 60)
    
    # Запускаем планировщики
    asyncio.create_task(admin_mailing_scheduler())
    asyncio.create_task(payment_checker())
    
    # Восстанавливаем активные рассылки
    await mailing_manager.start_all_mailings()
    
    logger.info("✅ Бот запущен, планировщики активны")

async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())