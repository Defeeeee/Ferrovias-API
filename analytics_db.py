import sqlite3
import os
import glob
from datetime import datetime, timedelta
import random

STATION_ORDER = [
    "Retiro", "Saldias", "Ciudad Universitaria", "A. del Valle", "Padilla",
    "Florida", "Munro", "Carapachay", "Villa Adelina", "Boulogne Sur Mer",
    "A. Montes", "Don Torcuato", "A. Sordeaux", "Villa de Mayo",
    "Los Polvorines", "Pablo Nogues", "Grand Bourg", "Tierras Altas",
    "Tortuguitas", "M. Alberti", "Del Viso", "Cecilia Grierson", "Villa Rosa"
]

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analytics.db")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

class AnalyticsDatabase:
    def __init__(self):
        self.db_path = DB_PATH
        self.init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 1. Create timetable schedules table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS timetable_schedules (
                    train_id TEXT,
                    station_name TEXT,
                    scheduled_time_str TEXT,
                    direction TEXT,
                    day_type TEXT,
                    PRIMARY KEY (train_id, station_name, day_type)
                )
            """)
            
            # 2. Create performance records table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_records (
                    id TEXT PRIMARY KEY,
                    train_id TEXT,
                    station_name TEXT,
                    scheduled_time TEXT,
                    actual_time TEXT,
                    delay_minutes INTEGER,
                    status TEXT,
                    direction TEXT,
                    date TEXT
                )
            """)
            
            # 3. Create a table to track scraping logs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraping_logs (
                    timestamp TEXT PRIMARY KEY,
                    stations_processed INTEGER,
                    records_logged INTEGER,
                    status TEXT,
                    error_msg TEXT
                )
            """)
            conn.commit()

        # Load CSV timetables if database schedules are empty
        if self.is_timetable_empty():
            print("Database timetable is empty. Loading CSV schedules...")
            self.load_csv_timetables()

    def is_timetable_empty(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM timetable_schedules")
            return cursor.fetchone()[0] == 0

    def is_performance_empty(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM performance_records")
            return cursor.fetchone()[0] == 0

    def load_csv_timetables(self):
        # Traverse all directories in data folder recursively
        csv_files = glob.glob(os.path.join(DATA_DIR, "**/*.csv"), recursive=True)
        if not csv_files:
            print("No CSV files found in data folder!")
            return

        total_inserted = 0
        records = []
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            day_type = filename.split('.')[0].rstrip('s') # weekday, saturday, sunday
            
            # Normalize path separators to search contents
            norm_path = file_path.replace("\\", "/").lower()
            
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split(" ")
                    if len(parts) < 2:
                        continue
                    
                    train_id = parts[0]
                    times = parts[1:]
                    
                    direction = None
                    actual_stations = []
                    starting_station_index = 0
                    
                    # Match paths and extract direction & stations
                    if "/boulogne/retiro/" in norm_path:
                        direction = 'retiro'
                        starting_station_index = STATION_ORDER.index("Boulogne Sur Mer")
                        actual_stations = list(reversed(STATION_ORDER[:starting_station_index + 1]))
                    elif "/boulogne/villarosa/" in norm_path:
                        direction = 'villarosa'
                        starting_station_index = STATION_ORDER.index("Boulogne Sur Mer")
                        actual_stations = STATION_ORDER[starting_station_index : starting_station_index + len(times)]
                    elif "/villarosa/" in norm_path:
                        direction = 'boulogne' if len(times) <= 11 else 'villarosa'
                        actual_stations = STATION_ORDER[:len(times)]
                    elif "/retiro/" in norm_path:
                        direction = 'retiro'
                        actual_stations = list(reversed(STATION_ORDER[len(STATION_ORDER)-len(times):]))
                    elif "/grandbourg/" in norm_path:
                        direction = 'retiro'
                        starting_station_index = STATION_ORDER.index("Grand Bourg")
                        actual_stations = list(reversed(STATION_ORDER[:starting_station_index + 1]))
                    else:
                        try:
                            tid_num = int(train_id)
                        except ValueError:
                            tid_num = 0
                        direction = 'villarosa' if tid_num % 2 == 0 else 'retiro'
                        if direction == 'villarosa':
                            actual_stations = STATION_ORDER[:len(times)]
                        else:
                            actual_stations = list(reversed(STATION_ORDER[len(STATION_ORDER)-len(times):]))
                            
                    for j, time_str in enumerate(times):
                        if j < len(actual_stations):
                            station_name = actual_stations[j]
                            records.append((train_id, station_name, time_str, direction, day_type))
                            
        if records:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany("""
                    INSERT OR REPLACE INTO timetable_schedules
                    (train_id, station_name, scheduled_time_str, direction, day_type)
                    VALUES (?, ?, ?, ?, ?)
                """, records)
                conn.commit()
                print(f"Loaded {len(records)} timetable schedules into database.")
    
    def log_arrival_record(self, train_id, station_name, destination):
        # Triggered when backend scraper identifies a train AT a station
        # Corrected to GMT-3 (subtract 3 hours from server time)
        now = datetime.now() - timedelta(hours=3)
        date_str = now.strftime("%Y-%m-%d")
        record_id = f"{date_str}-{station_name}-{train_id}"
        
        # Check if record already exists for today to prevent duplicates
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM performance_records WHERE id = ?", (record_id,))
            if cursor.fetchone():
                return False # Already logged
                
            # Determine schedule day type
            weekday = now.weekday()
            day_type = "sunday" if weekday == 6 else ("saturday" if weekday == 5 else "weekday")
            
            # Retrieve closest schedule matching this train/station
            cursor.execute("""
                SELECT scheduled_time_str, direction FROM timetable_schedules
                WHERE train_id = ? AND station_name = ? AND day_type = ?
            """, (train_id, station_name, day_type))
            row = cursor.fetchone()
            
            if not row:
                # No schedule found (e.g. custom or unscheduled train)
                return False
                
            sched_time_str, direction = row
            hours, mins = map(int, sched_time_str.split(':'))
            sched_dt = now.replace(hour=hours, minute=mins, second=0, microsecond=0)
            
            # If current time is early morning and scheduled time is late night, adjust date
            time_diff = now - sched_dt
            if time_diff.total_seconds() > 43200: # 12 hours
                sched_dt += timedelta(days=1)
            elif time_diff.total_seconds() < -43200:
                sched_dt -= timedelta(days=1)
                
            delay_minutes = int((now - sched_dt).total_seconds() / 60)
            
            if delay_minutes < -2:
                status = "early"
            elif delay_minutes > 5:
                status = "delayed"
            else:
                status = "on_time"
                
            cursor.execute("""
                INSERT OR REPLACE INTO performance_records
                (id, train_id, station_name, scheduled_time, actual_time, delay_minutes, status, direction, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record_id, train_id, station_name, sched_dt.isoformat(), 
                now.isoformat(), delay_minutes, status, direction, date_str
            ))
            conn.commit()
            print(f"Logged arrival: Train #{train_id} at {station_name}. Delay: {delay_minutes} min. Status: {status}")
            return True

    def write_scraping_log(self, processed_count, logged_count, status, error_msg=""):
        # Corrected to GMT-3
        timestamp_str = (datetime.now() - timedelta(hours=3)).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO scraping_logs
                (timestamp, stations_processed, records_logged, status, error_msg)
                VALUES (?, ?, ?, ?, ?)
            """, (timestamp_str, processed_count, logged_count, status, error_msg))
            conn.commit()

    def get_collection_status(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM performance_records")
            perf_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM scraping_logs")
            log_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT status, timestamp, error_msg FROM scraping_logs ORDER BY timestamp DESC LIMIT 1")
            last_run = cursor.fetchone()
            
            return {
                "recordsCount": perf_count,
                "logsCount": log_count,
                "lastRunStatus": last_run[0] if last_run else "N/A",
                "lastRunTime": last_run[1] if last_run else "N/A",
                "lastRunError": last_run[2] if last_run else ""
            }

    def get_analytics_stats(self, days=30):
        # Corrected to GMT-3
        now_local = datetime.now() - timedelta(hours=3)
        cutoff_date = (now_local - timedelta(days=days)).strftime("%Y-%m-%d")
        
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 1. Fetch system wide totals
            cursor.execute("""
                SELECT COUNT(*) as total_deps,
                       SUM(CASE WHEN status IN ('on_time', 'early') THEN 1 ELSE 0 END) as on_time_deps,
                       AVG(CASE WHEN delay_minutes > 0 THEN delay_minutes ELSE NULL END) as avg_delay
                FROM performance_records
                WHERE date >= ?
            """, (cutoff_date,))
            system_row = cursor.fetchone()
            
            total_departures = system_row['total_deps'] or 0
            if total_departures == 0:
                return {
                    "totalDepartures": 0,
                    "systemPunctuality": 0,
                    "averageSystemDelay": 0.0,
                    "bestPerformingStation": "N/A",
                    "worstPerformingStation": "N/A",
                    "peakHours": [],
                    "dataRange": {
                        "from": cutoff_date,
                        "to": now_local.strftime("%Y-%m-%d")
                    },
                    "standings": []
                }
                
            on_time_deps = system_row['on_time_deps'] or 0
            system_punctuality = int((on_time_deps / total_departures) * 100)
            avg_delay = round(system_row['avg_delay'] or 0, 1)
            
            # 2. Get standings per station
            cursor.execute("""
                SELECT station_name,
                       COUNT(*) as total,
                       SUM(CASE WHEN status IN ('on_time', 'early') THEN 1 ELSE 0 END) as on_time,
                       AVG(CASE WHEN delay_minutes > 0 THEN delay_minutes ELSE 0 END) as avg_del,
                       MAX(delay_minutes) as max_del
                FROM performance_records
                WHERE date >= ?
                GROUP BY station_name
            """, (cutoff_date,))
            
            station_rows = cursor.fetchall()
            station_stats = []
            
            for row in station_rows:
                pct = int((row['on_time'] / row['total']) * 100) if row['total'] > 0 else 100
                
                # Determine best hour by sorting hourly counts
                cursor.execute("""
                    SELECT strftime('%H:00', scheduled_time) as hr, COUNT(*) as hr_tot,
                           SUM(CASE WHEN status IN ('on_time', 'early') THEN 1 ELSE 0 END) as hr_ot
                    FROM performance_records
                    WHERE station_name = ? AND date >= ?
                    GROUP BY hr
                    ORDER BY (CAST(hr_ot AS FLOAT)/hr_tot) DESC
                    LIMIT 1
                """, (row['station_name'], cutoff_date))
                hr_row = cursor.fetchone()
                best_hour = hr_row['hr'] if hr_row else "08:00"
                
                station_stats.append({
                    "stationName": row['station_name'],
                    "totalDepartures": row['total'],
                    "onTimeDepartures": row['on_time'],
                    "averageDelayMinutes": round(row['avg_del'] or 0, 1),
                    "punctualityPercentage": pct,
                    "worstDelayMinutes": row['max_del'] or 0,
                    "bestPerformanceHour": best_hour,
                    "lastUpdated": now_local.isoformat()
                })
                
            # Sort by punctuality
            station_stats.sort(key=lambda s: s['punctualityPercentage'], reverse=True)
            
            best_station = station_stats[0]['stationName'] if station_stats else "N/A"
            worst_station = station_stats[-1]['stationName'] if station_stats else "N/A"
            
            # 3. Peak traffic hours
            cursor.execute("""
                SELECT strftime('%H:00', scheduled_time) as hr, COUNT(*) as cnt
                FROM performance_records
                WHERE date >= ?
                GROUP BY hr
                ORDER BY cnt DESC
                LIMIT 3
            """, (cutoff_date,))
            peak_hours = [r['hr'] for r in cursor.fetchall()]
            
            return {
                "totalDepartures": total_departures,
                "systemPunctuality": system_punctuality,
                "averageSystemDelay": avg_delay,
                "bestPerformingStation": best_station,
                "worstPerformingStation": worst_station,
                "peakHours": peak_hours if peak_hours else ["07:00", "08:00", "18:00"],
                "dataRange": {
                    "from": cutoff_date,
                    "to": now_local.strftime("%Y-%m-%d")
                },
                "standings": station_stats
            }
