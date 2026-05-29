# Save this as api.py
import httpx
import asyncio
import time
import sqlite3
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List
from fetching import fetch_train_data, STATION_IDS
from fastapi.middleware.cors import CORSMiddleware
from analytics_db import AnalyticsDatabase, STATION_ORDER

# In-memory caching for train status
CACHE_TTL_SECONDS = 30
cache_all_data = None
cache_last_updated = 0.0
cache_lock = asyncio.Lock()

# SQLite analytics database
db = AnalyticsDatabase()
is_scraping_active = True

app = FastAPI(
    title="Ferrovias Train API",
    description="An unofficial API to get train arrival times and analytics for the Belgrano Norte line.",
    version="1.1.0"
)

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000", # Next.js dev server
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
    "http://127.0.0.1:3000", # Next.js dev server
    "*",  # This is the origin for local files
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allows specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


# --- Background Worker Loop ---
async def background_collector_loop():
    """
    Runs in the background every 2 minutes. Scrapes all stations and logs
    train arrivals to SQLite database for continuous punctuality analytics.
    """
    global is_scraping_active
    print("Background analytics collector loop started.")
    
    # Wait a few seconds for FastAPI to finish startup
    await asyncio.sleep(5)
    
    while True:
        try:
            if is_scraping_active:
                print("Background Collector: Polling live station schedules...")
                # Fetch fresh or cached station values
                all_stations_cache = await get_cached_stations_data()
                
                logged_count = 0
                processed_count = 0
                
                for station_name, data in all_stations_cache.items():
                    all_deps = data.get("all_departures", {})
                    processed_count += 1
                    
                    for train_dest_id, time_data in all_deps.items():
                        if '-' not in train_dest_id:
                            continue
                        
                        destination, train_id = train_dest_id.split('-')
                        time_str = time_data[0] if isinstance(time_data, list) else time_data
                        
                        if time_str.lower() == "en estacion":
                            # Attempt to log arrival in SQLite
                            logged = db.log_arrival_record(train_id, station_name, destination)
                            if logged:
                                logged_count += 1
                                
                db.write_scraping_log(processed_count, logged_count, "SUCCESS")
                print(f"Background Collector: Polled {processed_count} stations. Logged {logged_count} new arrivals.")
            else:
                print("Background Collector: Idle (scraping disabled).")
        except Exception as e:
            print(f"Background Collector Error: {e}")
            db.write_scraping_log(0, 0, "ERROR", str(e))
            
        await asyncio.sleep(45) # Poll every 45 seconds to catch all trains


@app.on_event("startup")
async def startup_event():
    # Start the collector loop in the asyncio background
    asyncio.create_task(background_collector_loop())


# --- Endpoint 1: API Root ---
@app.get("/", summary="API Root", tags=["General"])
async def get_root():
    return {
        "message": "Welcome to the Ferrovias Train & Analytics API",
        "documentation": "/docs",
        "caching": "30-seconds TTL",
        "background_collector": "Active"
    }


# --- Endpoint 2: List All Stations ---
@app.get("/stations", summary="List All Stations", tags=["Stations"])
async def get_stations():
    return {"stations": STATION_IDS}


# --- Cache Helpers ---
async def fetch_with_semaphore(semaphore, client, name, station_id):
    async with semaphore:
        data = await fetch_train_data(client, station_id)
        if data.get("error"):
            print(f"Fetcher Error for {name}: {data.get('error')}")
        return name, data

async def get_all_stations_data_internal() -> dict:
    station_data_map = {}
    semaphore = asyncio.Semaphore(5) # Limit concurrency to 5 to avoid overwhelming the server
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for name, station_id in STATION_IDS.items():
            tasks.append(fetch_with_semaphore(semaphore, client, name, station_id))
        
        results = await asyncio.gather(*tasks)
        for name, data in results:
            if not data.get("error"):
                station_data_map[name] = data
                
    print(f"Fetcher: Successfully retrieved data for {len(station_data_map)}/{len(STATION_IDS)} stations.")
    return station_data_map

async def get_cached_stations_data() -> dict:
    global cache_all_data, cache_last_updated
    current_time = time.time()
    async with cache_lock:
        if cache_all_data is not None and (current_time - cache_last_updated) < CACHE_TTL_SECONDS:
            return cache_all_data
        
        cache_all_data = await get_all_stations_data_internal()
        cache_last_updated = current_time
        return cache_all_data


# --- Endpoint 3: Get Single Station Data ---
@app.get("/stations/{station_name}", summary="Get Data for One Station", tags=["Arrivals"])
async def get_station_arrivals(
        station_name: str,
        direction: Optional[str] = Query(None, enum=["retiro", "villarosa"], description="Filter by direction")
):
    station_id = STATION_IDS.get(station_name)
    if not station_id:
        raise HTTPException(status_code=404, detail="Station not found")

    all_stations_cache = await get_cached_stations_data()
    data = all_stations_cache.get(station_name)

    if not data:
        async with httpx.AsyncClient() as client:
            data = await fetch_train_data(client, station_id)
        if data.get("error"):
            raise HTTPException(status_code=500, detail=data["error"])

    if direction == "retiro":
        return data.get("to_retiro", {})
    if direction == "villarosa":
        return data.get("to_villa_rosa_branch", {})

    return data.get("all_departures", {})


# --- Endpoint 4: Get All Stations Concurrently ---
@app.get("/stations/all/status", summary="Get Data for All Stations", tags=["Arrivals"])
async def get_all_station_arrivals():
    all_stations_cache = await get_cached_stations_data()
    formatted_data = {}
    for name, data in all_stations_cache.items():
        formatted_data[name] = data.get("all_departures", {})
    return formatted_data


# --- Endpoint 5: Get Server-Side Analytics Stats ---
@app.get("/analytics/stats", summary="Get Punctuality Stats", tags=["Analytics"])
async def get_analytics_stats(days: int = Query(30, description="Calculate stats for past N days")):
    stats = db.get_analytics_stats(days)
    if stats is None:
        raise HTTPException(status_code=404, detail="No analytics data logs available yet.")
    return stats


# --- Endpoint 6: Get Collector Status ---
@app.get("/analytics/status", summary="Get Collector Status", tags=["Analytics"])
async def get_analytics_status():
    status = db.get_collection_status()
    status["isCollecting"] = is_scraping_active
    return status


# --- Endpoint 7: Toggle Collector Scraper ---
@app.post("/analytics/toggle", summary="Toggle Collector Scraper", tags=["Analytics"])
async def toggle_analytics_collector(active: bool):
    global is_scraping_active
    is_scraping_active = active
    print(f"Server-Side Scraping State changed to: {is_scraping_active}")
    return {"isCollecting": is_scraping_active, "message": f"Collector state set to {is_scraping_active}"}


# --- Endpoint 8: Route Planner ---
@app.get("/route", summary="Plan Train Route", tags=["Commuter"])
async def plan_route(
    origin: str = Query(..., description="Origin station name"),
    destination: str = Query(..., description="Destination station name")
):
    if origin not in STATION_ORDER or destination not in STATION_ORDER:
        raise HTTPException(status_code=400, detail="Invalid origin or destination station name.")
    
    orig_idx = STATION_ORDER.index(origin)
    dest_idx = STATION_ORDER.index(destination)
    
    if orig_idx == dest_idx:
        raise HTTPException(status_code=400, detail="Origin and destination must be different.")
    
    direction = 'villarosa' if orig_idx < dest_idx else 'retiro'
    
    now = datetime.now() - timedelta(hours=3)
    current_time_str = now.strftime("%H:%M")
    weekday = now.weekday()
    day_type = "sunday" if weekday == 6 else ("saturday" if weekday == 5 else "weekday")
    
    # Look back 60 minutes to capture delayed trains that haven't departed yet
    search_dt = now - timedelta(minutes=60)
    search_time_str = search_dt.strftime("%H:%M")
    
    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT s1.train_id, s1.scheduled_time_str AS origin_time, s2.scheduled_time_str AS dest_time
            FROM timetable_schedules s1
            JOIN timetable_schedules s2 ON s1.train_id = s2.train_id AND s1.day_type = s2.day_type
            WHERE s1.station_name = ? AND s2.station_name = ? AND s1.day_type = ?
              AND s1.scheduled_time_str < s2.scheduled_time_str
              AND s1.scheduled_time_str >= ?
            ORDER BY s1.scheduled_time_str ASC
            LIMIT 12
        """, (origin, destination, day_type, search_time_str))
        
        rows = cursor.fetchall()
        
    all_stations_cache = await get_cached_stations_data()
    
    routes = []
    for row in rows:
        train_id = row['train_id']
        origin_sched = row['origin_time']
        dest_sched = row['dest_time']
        
        if orig_idx < dest_idx:
            journey_stations = STATION_ORDER[orig_idx : dest_idx + 1]
        else:
            journey_stations = STATION_ORDER[dest_idx : orig_idx + 1][::-1]
            
        live_info = []
        for st_name, data in all_stations_cache.items():
            all_deps = data.get("all_departures", {})
            for key, val in all_deps.items():
                if f"-{train_id}" in key:
                    time_str = val[0] if isinstance(val, list) else val
                    live_info.append({
                        "station": st_name,
                        "status": time_str
                    })
                    
        delay_minutes = 0
        live_location = None
        status = "No Live Data"
        
        if live_info:
            at_station = [item for item in live_info if item['status'].lower() == "en estacion"]
            if at_station:
                live_location = f"At {at_station[0]['station']}"
                target_station = at_station[0]['station']
                with db.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT scheduled_time_str FROM timetable_schedules
                        WHERE train_id = ? AND station_name = ? AND day_type = ?
                    """, (train_id, target_station, day_type))
                    sched_row = cursor.fetchone()
                    if sched_row:
                        sched_str = sched_row[0]
                        try:
                            sh, sm = map(int, sched_str.split(':'))
                            sched_time = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                            delay_minutes = int((now - sched_time).total_seconds() / 60)
                            if delay_minutes < -2:
                                status = "Early"
                            elif delay_minutes > 5:
                                status = "Delayed"
                            else:
                                status = "On Time"
                        except Exception:
                            pass
            else:
                approaching = []
                for item in live_info:
                    try:
                        lower_s = item['status'].lower()
                        if "min" in lower_s:
                            mins = int(lower_s.split(' ')[0])
                            approaching.append((mins, item['station']))
                    except Exception:
                        pass
                if approaching:
                    approaching.sort()
                    closest_mins, closest_station = approaching[0]
                    live_location = f"Approaching {closest_station} ({closest_mins} min)"
                    status = "Running"
        
        try:
            oh, om = map(int, origin_sched.split(':'))
            origin_dt = now.replace(hour=oh, minute=om, second=0, microsecond=0) + timedelta(minutes=delay_minutes)
            
            # Handle midnight crossing date wrap-around
            time_diff = now - origin_dt
            if time_diff.total_seconds() > 43200: # 12 hours
                origin_dt += timedelta(days=1)
            elif time_diff.total_seconds() < -43200:
                origin_dt -= timedelta(days=1)
                
            est_departure = origin_dt.strftime("%H:%M")
            
            dh, dm = map(int, dest_sched.split(':'))
            dest_dt = now.replace(hour=dh, minute=dm, second=0, microsecond=0) + timedelta(minutes=delay_minutes)
            time_diff_dest = now - dest_dt
            if time_diff_dest.total_seconds() > 43200:
                dest_dt += timedelta(days=1)
            elif time_diff_dest.total_seconds() < -43200:
                dest_dt -= timedelta(days=1)
                
            est_arrival = dest_dt.strftime("%H:%M")
        except Exception:
            origin_dt = now
            est_departure = origin_sched
            est_arrival = dest_sched
            
        # Filter out trains that have already departed (with a 2-minute boarding margin)
        if origin_dt < now - timedelta(minutes=2):
            continue
            
        stops = []
        with db.get_connection() as conn:
            cursor = conn.cursor()
            for st in journey_stations:
                cursor.execute("""
                    SELECT scheduled_time_str FROM timetable_schedules
                    WHERE train_id = ? AND station_name = ? AND day_type = ?
                """, (train_id, st, day_type))
                st_row = cursor.fetchone()
                st_sched = st_row[0] if st_row else "--:--"
                
                is_current = False
                if live_location and st in live_location:
                    is_current = True
                    
                stops.append({
                    "stationName": st,
                    "scheduledTime": st_sched,
                    "isCurrent": is_current
                })
                
        try:
            oh, om = map(int, origin_sched.split(':'))
            dh, dm = map(int, dest_sched.split(':'))
            dur_mins = (dh * 60 + dm) - (oh * 60 + om)
            if dur_mins < 0:
                dur_mins += 24 * 60
            duration_str = f"{dur_mins} min"
        except Exception:
            duration_str = "N/A"
            
        routes.append({
            "trainId": train_id,
            "scheduledDeparture": origin_sched,
            "scheduledArrival": dest_sched,
            "estimatedDeparture": est_departure,
            "estimatedArrival": est_arrival,
            "delayMinutes": max(0, delay_minutes),
            "status": status,
            "liveLocation": live_location,
            "duration": duration_str,
            "stops": stops
        })
        
    return {
        "origin": origin,
        "destination": destination,
        "dayType": day_type,
        "currentTime": current_time_str,
        "trains": routes[:4]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8085)