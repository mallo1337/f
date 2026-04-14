# database.py
import psycopg2
import os
import logging
from datetime import datetime, timedelta
import random
import string
import re

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.connect_with_retry()
    
    def connect_with_retry(self, max_retries=3, delay=2):
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                self.cursor = self.conn.cursor()
                self.create_tables()
                self.create_indexes()
                logger.info("Database initialized successfully")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    import time
                    time.sleep(delay)
                else:
                    logger.error(f"All database connection attempts failed: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error initializing database: {e}")
                raise
    
    def create_tables(self):
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS players (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    nickname TEXT,
                    game_id TEXT UNIQUE,
                    registration_date TEXT
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_stats (
                    user_id BIGINT PRIMARY KEY REFERENCES players(user_id),
                    rating INTEGER DEFAULT 0,
                    matches_played INTEGER DEFAULT 0,
                    kills INTEGER DEFAULT 0,
                    deaths INTEGER DEFAULT 0,
                    CONSTRAINT rating_non_negative CHECK (rating >= 0),
                    CONSTRAINT matches_non_negative CHECK (matches_played >= 0),
                    CONSTRAINT kills_non_negative CHECK (kills >= 0),
                    CONSTRAINT deaths_non_negative CHECK (deaths >= 0)
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weekly_stats (
                    user_id BIGINT REFERENCES players(user_id),
                    week_start DATE,
                    rating_points INTEGER DEFAULT 0,
                    kills INTEGER DEFAULT 0,
                    deaths INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, week_start),
                    CONSTRAINT weekly_rating_non_negative CHECK (rating_points >= 0),
                    CONSTRAINT weekly_kills_non_negative CHECK (kills >= 0),
                    CONSTRAINT weekly_deaths_non_negative CHECK (deaths >= 0)
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS lobbies (
                    lobby_id SERIAL PRIMARY KEY,
                    lobby_unique_id TEXT UNIQUE,
                    creator_id BIGINT REFERENCES players(user_id),
                    lobby_link TEXT,
                    mode TEXT,
                    map_name TEXT,
                    time_limit TEXT,
                    damage_type TEXT,
                    region TEXT,
                    max_players INTEGER DEFAULT 10,
                    current_players INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'active',
                    created_at TEXT,
                    topic_thread_id INTEGER,
                    channel_message_id INTEGER,
                    CONSTRAINT max_players_positive CHECK (max_players > 0),
                    CONSTRAINT current_players_non_negative CHECK (current_players >= 0),
                    CONSTRAINT valid_status CHECK (status IN ('active', 'completed', 'closed'))
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS lobby_players (
                    id SERIAL PRIMARY KEY,
                    lobby_id INTEGER REFERENCES lobbies(lobby_id) ON DELETE CASCADE,
                    user_id BIGINT REFERENCES players(user_id),
                    joined_at TEXT,
                    UNIQUE(lobby_id, user_id)
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS screenshots (
                    screenshot_id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES players(user_id),
                    lobby_id INTEGER REFERENCES lobbies(lobby_id) ON DELETE CASCADE,
                    topic_thread_id INTEGER,
                    created_at TEXT,
                    status TEXT DEFAULT 'pending',
                    CONSTRAINT valid_screenshot_status CHECK (status IN ('pending', 'processed', 'rejected'))
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS stats_history (
                    history_id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES players(user_id),
                    screenshot_id INTEGER REFERENCES screenshots(screenshot_id) ON DELETE CASCADE,
                    kills_added INTEGER,
                    deaths_added INTEGER,
                    rating_added INTEGER,
                    created_at TEXT,
                    lobby_id INTEGER REFERENCES lobbies(lobby_id) ON DELETE CASCADE,
                    UNIQUE(user_id, lobby_id),
                    CONSTRAINT history_kills_non_negative CHECK (kills_added >= 0),
                    CONSTRAINT history_deaths_non_negative CHECK (deaths_added >= 0),
                    CONSTRAINT history_rating_non_negative CHECK (rating_added >= 0)
                )
            ''')
            
            self.conn.commit()
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.conn.rollback()
            raise
    
    def create_indexes(self):
        try:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_player_stats_rating ON player_stats(rating)",
                "CREATE INDEX IF NOT EXISTS idx_player_stats_matches ON player_stats(matches_played)",
                "CREATE INDEX IF NOT EXISTS idx_lobbies_status ON lobbies(status)",
                "CREATE INDEX IF NOT EXISTS idx_lobbies_created ON lobbies(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_lobby_players_user_id ON lobby_players(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_lobby_players_lobby_id ON lobby_players(lobby_id)",
                "CREATE INDEX IF NOT EXISTS idx_screenshots_user_lobby ON screenshots(user_id, lobby_id)",
                "CREATE INDEX IF NOT EXISTS idx_screenshots_status ON screenshots(status)",
                "CREATE INDEX IF NOT EXISTS idx_stats_history_user_lobby ON stats_history(user_id, lobby_id)",
                "CREATE INDEX IF NOT EXISTS idx_weekly_stats_week ON weekly_stats(week_start)",
                "CREATE INDEX IF NOT EXISTS idx_players_nickname ON players(nickname)",
                "CREATE INDEX IF NOT EXISTS idx_players_game_id ON players(game_id)",
                "CREATE INDEX IF NOT EXISTS idx_lobbies_unique_id ON lobbies(lobby_unique_id)",
                "CREATE INDEX IF NOT EXISTS idx_lobbies_topic_thread ON lobbies(topic_thread_id)"
            ]
            
            for index_sql in indexes:
                self.cursor.execute(index_sql)
            
            self.conn.commit()
            logger.info("Indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            self.conn.rollback()
    
    def ensure_connection(self):
        try:
            self.cursor.execute("SELECT 1")
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            logger.warning("Database connection lost, reconnecting...")
            self.close_connection()
            self.connect_with_retry()
        except Exception as e:
            logger.warning(f"Connection check failed: {e}, reconnecting...")
            self.close_connection()
            self.connect_with_retry()

    def clear_all_stats(self):
        try:
            self.ensure_connection()
            self.cursor.execute("DELETE FROM stats_history")
            self.cursor.execute("DELETE FROM screenshots")
            self.cursor.execute("DELETE FROM lobby_players")
            self.cursor.execute("DELETE FROM lobbies")
            self.cursor.execute("DELETE FROM weekly_stats")
            self.cursor.execute("UPDATE player_stats SET rating = 0, matches_played = 0, kills = 0, deaths = 0")
            self.conn.commit()
            logger.info("All stats cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error clearing stats: {e}")
            self.conn.rollback()
            return False
    
    def clear_weekly_stats(self):
        try:
            self.ensure_connection()
            self.cursor.execute("DELETE FROM weekly_stats")
            self.conn.commit()
            logger.info("Weekly stats cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error clearing weekly stats: {e}")
            self.conn.rollback()
            return False

    def clear_lobbies_only(self):
        try:
            self.ensure_connection()
            self.cursor.execute("DELETE FROM lobbies")
            self.conn.commit()
            logger.info("Lobbies cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error clearing lobbies: {e}")
            self.conn.rollback()
            return False

    def delete_lobby(self, lobby_id):
        try:
            self.ensure_connection()
            
            # Сначала получаем информацию о лобби и игроках
            self.cursor.execute('SELECT lobby_unique_id FROM lobbies WHERE lobby_id = %s', (lobby_id,))
            lobby_result = self.cursor.fetchone()
            lobby_unique_id = lobby_result[0] if lobby_result else "?"
            
            self.cursor.execute('SELECT user_id FROM lobby_players WHERE lobby_id = %s', (lobby_id,))
            players = self.cursor.fetchall()
            player_ids = [player[0] for player in players]
            
            # Теперь удаляем лобби (каскадно удалятся lobby_players, screenshots, stats_history)
            self.cursor.execute("DELETE FROM lobbies WHERE lobby_id = %s", (lobby_id,))
            self.conn.commit()
            
            logger.info(f"Lobby {lobby_id} deleted successfully")
            return True, lobby_unique_id, player_ids
        except Exception as e:
            logger.error(f"Error deleting lobby {lobby_id}: {e}")
            self.conn.rollback()
            return False, "?", []
    
    def is_nickname_taken(self, nickname):
        # Убрана проверка на занятость ника
        return False

    def is_game_id_taken(self, game_id):
        try:
            self.ensure_connection()
            self.cursor.execute("SELECT 1 FROM players WHERE game_id = %s", (game_id,))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking game_id {game_id}: {e}")
            return True

    def is_user_registered(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute("SELECT 1 FROM players WHERE user_id = %s", (user_id,))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking user registration {user_id}: {e}")
            return False

    def register_player(self, user_id, username, nickname, game_id):
        if not re.match(r'^[a-zA-Z0-9_]{3,16}$', nickname):
            return False, "Никнейм должен содержать только английские буквы, цифры и подчеркивания (3-16 символов)"
        
        if not game_id.isdigit() or not (2 <= len(game_id) <= 13):
            return False, "Игровой ID должен содержать только цифры (от 2 до 13 символов)"
        
        if self.is_game_id_taken(game_id):
            return False, "Этот игровой ID уже занят"
        
        registration_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.ensure_connection()
            self.cursor.execute(
                "INSERT INTO players (user_id, username, nickname, game_id, registration_date) VALUES (%s, %s, %s, %s, %s)",
                (user_id, username, nickname, game_id, registration_date)
            )
            self.cursor.execute(
                "INSERT INTO player_stats (user_id) VALUES (%s)",
                (user_id,)
            )
            self.conn.commit()
            logger.info(f"User {user_id} registered successfully with nickname {nickname} and game_id {game_id}")
            return True, "Успешная регистрация"
        except psycopg2.Error as e:
            logger.error(f"Database error in register_player for user {user_id}: {e}")
            self.conn.rollback()
            return False, "Ошибка базы данных при регистрации"
        except Exception as e:
            logger.error(f"Unexpected error in register_player for user {user_id}: {e}")
            self.conn.rollback()
            return False, f"Внутренняя ошибка: {str(e)}"

    def get_player_profile(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT p.user_id, p.username, p.nickname, p.game_id, p.registration_date,
                       ps.rating, ps.matches_played, ps.kills, ps.deaths
                FROM players p
                LEFT JOIN player_stats ps ON p.user_id = ps.user_id
                WHERE p.user_id = %s
            ''', (user_id,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting player profile for {user_id}: {e}")
            return None
    
    def get_player_by_id(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute("SELECT nickname FROM players WHERE user_id = %s", (user_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting player by ID {user_id}: {e}")
            return None

    def get_player_game_id(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute("SELECT game_id FROM players WHERE user_id = %s", (user_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting player game_id for {user_id}: {e}")
            return None

    def get_weekly_top_players(self):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT p.user_id, p.nickname, ps.rating, ps.kills, ps.deaths, ps.matches_played
                FROM players p
                JOIN player_stats ps ON p.user_id = ps.user_id
                WHERE ps.matches_played > 0
                ORDER BY ps.rating DESC 
                LIMIT 10
            ''')
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting weekly top players: {e}")
            return []

    def get_all_time_top_players(self):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT p.user_id, p.nickname, ps.rating, ps.matches_played, ps.kills, ps.deaths
                FROM players p
                JOIN player_stats ps ON p.user_id = ps.user_id
                WHERE ps.matches_played > 0
                ORDER BY ps.rating DESC 
                LIMIT 10
            ''')
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting all time top players: {e}")
            return []

    def get_player_weekly_position(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT matches_played FROM player_stats WHERE user_id = %s', (user_id,))
            player_stats = self.cursor.fetchone()
            
            if not player_stats or player_stats[0] == 0:
                return 0
            
            self.cursor.execute('''
                SELECT COUNT(*) + 1
                FROM players p1
                JOIN player_stats ps1 ON p1.user_id = ps1.user_id
                WHERE ps1.rating > (
                    SELECT ps2.rating 
                    FROM player_stats ps2 
                    WHERE ps2.user_id = %s
                )
                AND ps1.matches_played > 0
            ''', (user_id,))
            result = self.cursor.fetchone()
            
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting weekly position for user {user_id}: {e}")
            return 0

    def get_player_all_time_position(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT COUNT(*) + 1
                FROM players p1
                JOIN player_stats ps1 ON p1.user_id = ps1.user_id
                WHERE ps1.rating > (
                    SELECT ps2.rating 
                    FROM player_stats ps2 
                    WHERE ps2.user_id = %s
                )
                AND ps1.matches_played > 0
            ''', (user_id,))
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting all time position for user {user_id}: {e}")
            return 0

    def get_player_has_any_stats(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT matches_played FROM player_stats WHERE user_id = %s', (user_id,))
            result = self.cursor.fetchone()
            return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Error checking stats for user {user_id}: {e}")
            return False

    def create_lobby(self, creator_id, lobby_link, mode, map_name, time_limit, damage_type, region, max_players=10):
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.ensure_connection()
            
            lobby_unique_id = ''.join(random.choices(string.digits + string.ascii_uppercase, k=5))
            
            self.cursor.execute('''
                INSERT INTO lobbies (creator_id, lobby_link, mode, map_name, time_limit, damage_type, region, max_players, created_at, lobby_unique_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING lobby_id
            ''', (creator_id, lobby_link, mode, map_name, time_limit, damage_type, region, max_players, created_at, lobby_unique_id))
            
            lobby_id = self.cursor.fetchone()[0]
            
            self.cursor.execute('''
                INSERT INTO lobby_players (lobby_id, user_id, joined_at)
                VALUES (%s, %s, %s)
            ''', (lobby_id, creator_id, created_at))
            
            self.conn.commit()
            
            logger.info(f"Lobby {lobby_id} created successfully by user {creator_id}")
            return lobby_id, lobby_unique_id
        except Exception as e:
            logger.error(f"Error creating lobby for user {creator_id}: {e}")
            self.conn.rollback()
            return None, None

    def get_active_lobbies(self):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT 
                    l.lobby_id,
                    l.lobby_unique_id,
                    l.creator_id,
                    l.lobby_link,
                    l.mode,
                    l.map_name,
                    l.time_limit,
                    l.damage_type,
                    l.region,
                    l.max_players,
                    l.current_players,
                    l.status,
                    l.created_at,
                    p.nickname as creator_name,
                    p.username as creator_username,
                    COUNT(lp.user_id) as player_count
                FROM lobbies l
                JOIN players p ON l.creator_id = p.user_id
                LEFT JOIN lobby_players lp ON l.lobby_id = lp.lobby_id
                WHERE l.status = 'active'
                GROUP BY l.lobby_id, p.nickname, p.username
                ORDER BY l.created_at DESC
            ''')
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting active lobbies: {e}")
            return []

    def get_lobby_by_id(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT 
                    l.lobby_id,
                    l.lobby_unique_id,
                    l.creator_id,
                    l.lobby_link,
                    l.mode,
                    l.map_name,
                    l.time_limit,
                    l.damage_type,
                    l.region,
                    l.max_players,
                    l.current_players,
                    l.status,
                    l.created_at,
                    p.nickname as creator_name,
                    p.username as creator_username,
                    l.topic_thread_id,
                    l.channel_message_id
                FROM lobbies l
                JOIN players p ON l.creator_id = p.user_id
                WHERE l.lobby_id = %s
            ''', (lobby_id,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting lobby by ID {lobby_id}: {e}")
            return None

    def get_lobby_players(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT p.user_id, p.nickname
                FROM lobby_players lp
                JOIN players p ON lp.user_id = p.user_id
                WHERE lp.lobby_id = %s
                ORDER BY lp.joined_at
            ''', (lobby_id,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting lobby players for lobby {lobby_id}: {e}")
            return []

    def join_lobby(self, user_id, lobby_id):
        try:
            self.ensure_connection()
            
            self.cursor.execute('SELECT 1 FROM lobby_players WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
            if self.cursor.fetchone():
                return False, "Вы уже в этом лобби"
            
            self.cursor.execute('SELECT COUNT(*) FROM lobby_players WHERE lobby_id = %s', (lobby_id,))
            player_count = self.cursor.fetchone()[0]
            
            self.cursor.execute('SELECT max_players FROM lobbies WHERE lobby_id = %s', (lobby_id,))
            max_players_result = self.cursor.fetchone()
            
            if not max_players_result:
                return False, "Лобби не найдено"
                
            max_players = max_players_result[0]
            
            if player_count >= max_players:
                return False, "Лобби заполнено"
            
            joined_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute('INSERT INTO lobby_players (lobby_id, user_id, joined_at) VALUES (%s, %s, %s)', (lobby_id, user_id, joined_at))
            self.cursor.execute('UPDATE lobbies SET current_players = current_players + 1 WHERE lobby_id = %s', (lobby_id,))
            
            self.conn.commit()
            
            logger.info(f"User {user_id} joined lobby {lobby_id}")
            return True, "Успешно присоединились к лобби"
        except psycopg2.Error as e:
            logger.error(f"Database error joining lobby {lobby_id} for user {user_id}: {e}")
            self.conn.rollback()
            return False, f"Ошибка базы данных: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error joining lobby {lobby_id} for user {user_id}: {e}")
            self.conn.rollback()
            return False, f"Внутренняя ошибка: {str(e)}"

    def leave_lobby(self, user_id, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('DELETE FROM lobby_players WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
            self.cursor.execute('UPDATE lobbies SET current_players = GREATEST(0, current_players - 1) WHERE lobby_id = %s', (lobby_id,))
            
            self.cursor.execute('SELECT COUNT(*) FROM lobby_players WHERE lobby_id = %s', (lobby_id,))
            player_count = self.cursor.fetchone()[0]
            
            if player_count == 0:
                self.cursor.execute('UPDATE lobbies SET status = %s WHERE lobby_id = %s', ('closed', lobby_id))
            
            self.conn.commit()
            logger.info(f"User {user_id} left lobby {lobby_id}")
            return True
        except Exception as e:
            logger.error(f"Error leaving lobby {lobby_id} for user {user_id}: {e}")
            self.conn.rollback()
            return False

    def is_user_in_lobby(self, user_id, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT 1 FROM lobby_players WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if user {user_id} is in lobby {lobby_id}: {e}")
            return False

    def get_user_active_lobby(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT l.lobby_id 
                FROM lobbies l
                JOIN lobby_players lp ON l.lobby_id = lp.lobby_id
                WHERE lp.user_id = %s AND l.status = 'active' AND l.current_players < l.max_players
            ''', (user_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting active lobby for user {user_id}: {e}")
            return None

    def complete_lobby(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('UPDATE lobbies SET status = %s WHERE lobby_id = %s', ('completed', lobby_id))
            self.conn.commit()
            logger.info(f"Lobby {lobby_id} completed")
            return True
        except Exception as e:
            logger.error(f"Error completing lobby {lobby_id}: {e}")
            self.conn.rollback()
            return False

    def get_lobby_info_for_channel(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT 
                    l.lobby_unique_id,
                    l.mode,
                    l.map_name,
                    l.time_limit,
                    l.damage_type,
                    l.region,
                    p.username,
                    l.creator_id
                FROM lobbies l
                JOIN players p ON l.creator_id = p.user_id
                WHERE l.lobby_id = %s
            ''', (lobby_id,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting lobby info for channel for lobby {lobby_id}: {e}")
            return None

    def add_screenshot_to_lobby(self, user_id, lobby_id, topic_thread_id):
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.ensure_connection()
            self.cursor.execute('''
                INSERT INTO screenshots (user_id, lobby_id, topic_thread_id, created_at) 
                VALUES (%s, %s, %s, %s)
                RETURNING screenshot_id
            ''', (user_id, lobby_id, topic_thread_id, created_at))
            screenshot_id = self.cursor.fetchone()[0]
            self.conn.commit()
            logger.info(f"Screenshot {screenshot_id} added for user {user_id} in lobby {lobby_id}")
            return screenshot_id
        except Exception as e:
            logger.error(f"Error adding screenshot for user {user_id} in lobby {lobby_id}: {e}")
            self.conn.rollback()
            return None

    def get_screenshots_by_lobby(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT s.screenshot_id, s.user_id, s.status, p.nickname
                FROM screenshots s
                JOIN players p ON s.user_id = p.user_id
                WHERE s.lobby_id = %s
                ORDER BY s.created_at
            ''', (lobby_id,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting screenshots for lobby {lobby_id}: {e}")
            return []

    def has_player_submitted_screenshot(self, user_id, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT 1 FROM screenshots WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking screenshot for user {user_id} in lobby {lobby_id}: {e}")
            return False

    def update_screenshot_status(self, screenshot_id, status):
        try:
            self.ensure_connection()
            self.cursor.execute('UPDATE screenshots SET status = %s WHERE screenshot_id = %s', (status, screenshot_id))
            self.conn.commit()
            logger.info(f"Screenshot {screenshot_id} status updated to {status}")
            return True
        except Exception as e:
            logger.error(f"Error updating screenshot {screenshot_id} status: {e}")
            self.conn.rollback()
            return False

    def has_stats_been_added(self, user_id, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT 1 FROM stats_history WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking stats history for user {user_id} in lobby {lobby_id}: {e}")
            return True

    def add_stats_history_with_lobby(self, user_id, screenshot_id, kills_added, deaths_added, rating_added, lobby_id):
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.ensure_connection()
            self.cursor.execute('''
                INSERT INTO stats_history (user_id, screenshot_id, kills_added, deaths_added, rating_added, created_at, lobby_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (user_id, screenshot_id, kills_added, deaths_added, rating_added, created_at, lobby_id))
            self.conn.commit()
            logger.info(f"Stats history added for user {user_id} in lobby {lobby_id}")
            return True
        except Exception as e:
            logger.error(f"Error adding stats history for user {user_id} in lobby {lobby_id}: {e}")
            self.conn.rollback()
            return False

    def get_last_stats_history_by_lobby_user(self, lobby_id, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT history_id, user_id, screenshot_id, kills_added, deaths_added, rating_added, created_at
                FROM stats_history 
                WHERE lobby_id = %s AND user_id = %s
                ORDER BY history_id DESC 
                LIMIT 1
            ''', (lobby_id, user_id))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting last stats history for user {user_id} in lobby {lobby_id}: {e}")
            return None

    def update_player_stats_by_user_id(self, user_id, kills=0, deaths=0, lobby_id=None):
        try:
            self.ensure_connection()
            
            if kills < 1 or kills > 1000:
                logger.warning(f"Invalid kills value {kills} for user {user_id}")
                return False, 0
            if deaths < 1 or deaths > 1000:
                logger.warning(f"Invalid deaths value {deaths} for user {user_id}")
                return False, 0
                
            if lobby_id:
                self.cursor.execute('SELECT 1 FROM stats_history WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
                if self.cursor.fetchone():
                    logger.warning(f"Stats already added for user {user_id} in lobby {lobby_id}")
                    return False, 0
            
            self.cursor.execute('SELECT rating, matches_played, kills, deaths FROM player_stats WHERE user_id = %s', (user_id,))
            current_stats = self.cursor.fetchone()
            
            if not current_stats:
                logger.warning(f"No player stats found for user {user_id}")
                return False, 0
                
            current_rating, current_matches, current_kills, current_deaths = current_stats
            
            rating_to_add = kills + 1
            new_rating = current_rating + rating_to_add
            new_matches = current_matches + 1
            new_kills = current_kills + kills
            new_deaths = current_deaths + deaths
            
            self.cursor.execute('''
                UPDATE player_stats 
                SET rating = %s, matches_played = %s, kills = %s, deaths = %s
                WHERE user_id = %s
            ''', (new_rating, new_matches, new_kills, new_deaths, user_id))
            
            screenshot_id = None
            self.cursor.execute('SELECT screenshot_id FROM screenshots WHERE user_id = %s AND lobby_id = %s', (user_id, lobby_id))
            screenshot_result = self.cursor.fetchone()
            if screenshot_result:
                screenshot_id = screenshot_result[0]
            
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute('''
                INSERT INTO stats_history (user_id, screenshot_id, kills_added, deaths_added, rating_added, created_at, lobby_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (user_id, screenshot_id, kills, deaths, rating_to_add, created_at, lobby_id))
            
            if screenshot_id:
                self.cursor.execute('UPDATE screenshots SET status = %s WHERE screenshot_id = %s', ('processed', screenshot_id))
            
            today = datetime.now()
            start_of_week = today - timedelta(days=today.weekday())
            start_of_week_str = start_of_week.strftime("%Y-%m-%d")
            
            self.cursor.execute('''
                INSERT INTO weekly_stats (user_id, week_start, rating_points, kills, deaths)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, week_start) 
                DO UPDATE SET 
                    rating_points = weekly_stats.rating_points + %s,
                    kills = weekly_stats.kills + %s,
                    deaths = weekly_stats.deaths + %s
            ''', (user_id, start_of_week_str, rating_to_add, kills, deaths, rating_to_add, kills, deaths))
            
            self.conn.commit()
            
            logger.info(f"Stats updated for user {user_id}: +{kills}k/{deaths}d, +{rating_to_add} rating")
            return True, rating_to_add
        except psycopg2.Error as e:
            logger.error(f"Database error updating stats for user {user_id}: {e}")
            self.conn.rollback()
            return False, 0
        except Exception as e:
            logger.error(f"Unexpected error updating stats for user {user_id}: {e}")
            self.conn.rollback()
            return False, 0

    def revert_stats(self, history_id):
        try:
            self.ensure_connection()
            
            self.cursor.execute('''
                SELECT sh.history_id, sh.user_id, sh.screenshot_id, sh.kills_added, sh.deaths_added, sh.rating_added, sh.lobby_id
                FROM stats_history sh
                WHERE sh.history_id = %s
            ''', (history_id,))
            history_data = self.cursor.fetchone()
            
            if not history_data:
                logger.warning(f"History record {history_id} not found")
                return False
                
            history_id, user_id, screenshot_id, kills_added, deaths_added, rating_added, lobby_id = history_data
            
            self.cursor.execute('SELECT rating, matches_played, kills, deaths FROM player_stats WHERE user_id = %s', (user_id,))
            current_stats = self.cursor.fetchone()
            
            if not current_stats:
                logger.warning(f"No current stats found for user {user_id} when reverting")
                return False
                
            current_rating, current_matches, current_kills, current_deaths = current_stats
            
            new_rating = max(0, current_rating - rating_added)
            new_matches = max(0, current_matches - 1)
            new_kills = max(0, current_kills - kills_added)
            new_deaths = max(0, current_deaths - deaths_added)
            
            self.cursor.execute('''
                UPDATE player_stats 
                SET rating = %s, matches_played = %s, kills = %s, deaths = %s
                WHERE user_id = %s
            ''', (new_rating, new_matches, new_kills, new_deaths, user_id))
            
            if screenshot_id:
                self.cursor.execute('UPDATE screenshots SET status = %s WHERE screenshot_id = %s', ('pending', screenshot_id))
            
            self.cursor.execute('DELETE FROM stats_history WHERE history_id = %s', (history_id,))
            
            today = datetime.now()
            start_of_week = today - timedelta(days=today.weekday())
            start_of_week_str = start_of_week.strftime("%Y-%m-%d")
            
            self.cursor.execute('''
                UPDATE weekly_stats 
                SET rating_points = GREATEST(0, rating_points - %s),
                    kills = GREATEST(0, kills - %s),
                    deaths = GREATEST(0, deaths - %s)
                WHERE user_id = %s AND week_start = %s
            ''', (rating_added, kills_added, deaths_added, user_id, start_of_week_str))
            
            self.conn.commit()
            
            logger.info(f"Stats reverted for user {user_id} from history {history_id}")
            return True
        except Exception as e:
            logger.error(f"Error reverting stats for history {history_id}: {e}")
            self.conn.rollback()
            return False

    def get_lobby_topic_thread_id(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT topic_thread_id FROM lobbies WHERE lobby_id = %s', (lobby_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting topic thread ID for lobby {lobby_id}: {e}")
            return None

    def update_lobby_topic_thread_id(self, lobby_id, topic_thread_id):
        try:
            self.ensure_connection()
            self.cursor.execute('UPDATE lobbies SET topic_thread_id = %s WHERE lobby_id = %s', (topic_thread_id, lobby_id))
            self.conn.commit()
            logger.info(f"Topic thread ID updated for lobby {lobby_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating topic thread ID for lobby {lobby_id}: {e}")
            self.conn.rollback()
            return False

    def get_lobby_id_by_topic_thread_id(self, topic_thread_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT lobby_id FROM lobbies WHERE topic_thread_id = %s', (topic_thread_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting lobby ID by topic thread {topic_thread_id}: {e}")
            return None

    def get_player_lobby_history(self, user_id, offset=0, limit=5):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT 
                    l.lobby_id,
                    l.lobby_unique_id,
                    l.mode,
                    l.map_name,
                    l.created_at,
                    COALESCE(sh.kills_added, 0) as kills_added,
                    COALESCE(sh.deaths_added, 0) as deaths_added,
                    COALESCE(sh.rating_added, 0) as rating_added,
                    COALESCE(sh.created_at, l.created_at) as stats_date,
                    CASE WHEN sh.history_id IS NOT NULL THEN true ELSE false END as has_stats
                FROM lobbies l
                JOIN lobby_players lp ON l.lobby_id = lp.lobby_id
                LEFT JOIN stats_history sh ON l.lobby_id = sh.lobby_id AND sh.user_id = %s
                WHERE lp.user_id = %s 
                AND l.status = 'completed'
                ORDER BY l.created_at DESC
                LIMIT %s OFFSET %s
            ''', (user_id, user_id, limit, offset))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting lobby history for user {user_id}: {e}")
            return []

    def get_player_lobby_history_count(self, user_id):
        try:
            self.ensure_connection()
            self.cursor.execute('''
                SELECT COUNT(*)
                FROM lobbies l
                JOIN lobby_players lp ON l.lobby_id = lp.lobby_id
                WHERE lp.user_id = %s 
                AND l.status = 'completed'
            ''', (user_id,))
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting lobby history count for user {user_id}: {e}")
            return 0

    def update_lobby_channel_message_id(self, lobby_id, message_id):
        try:
            self.ensure_connection()
            self.cursor.execute('UPDATE lobbies SET channel_message_id = %s WHERE lobby_id = %s', (message_id, lobby_id))
            self.conn.commit()
            logger.info(f"Channel message ID updated for lobby {lobby_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating channel message ID for lobby {lobby_id}: {e}")
            self.conn.rollback()
            return False

    def get_lobby_channel_message_id(self, lobby_id):
        try:
            self.ensure_connection()
            self.cursor.execute('SELECT channel_message_id FROM lobbies WHERE lobby_id = %s', (lobby_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting channel message ID for lobby {lobby_id}: {e}")
            return None

    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    def __del__(self):
        self.close_connection()