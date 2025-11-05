# Save this as api.py
import httpx
import asyncio
from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List
from fetching import fetch_train_data, STATION_IDS
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI(
    title="Ferrovias Train API",
    description="An unofficial API to get train arrival times for the Belgrano Norte line.",
    version="1.0.0"
)

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
    "null",  # This is the origin for local files
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allows specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


# --- Endpoint 1: API Root ---
@app.get("/", summary="API Root", tags=["General"])
async def get_root():
    """
    Root endpoint. Provides basic API info and a link to the docs.
    """
    return {
        "message": "Welcome to the Ferrovias Train API",
        "documentation": "/docs"
    }


# --- Endpoint 2: List All Stations ---
@app.get("/stations", summary="List All Stations", tags=["Stations"])
async def get_stations():
    """
    Get a list of all available station names and their IDs.
    This is perfect for populating a dropdown or picker in an app.
    """
    # Flip the dict to be "Name: ID"
    return {"stations": STATION_IDS}


# --- Endpoint 3: Get Single Station Data ---
@app.get("/stations/{station_name}", summary="Get Data for One Station", tags=["Arrivals"])
async def get_station_arrivals(
        station_name: str,
        direction: Optional[str] = Query(None, enum=["retiro", "villarosa"], description="Filter by direction")
):
    """
    Get arrival times for a specific station by its name.

    You can optionally filter by **direction**:
    - `?direction=retiro`
    - `?direction=villarosa` (for all non-Retiro destinations)
    """
    station_id = STATION_IDS.get(station_name)
    if not station_id:
        raise HTTPException(status_code=404, detail="Station not found")

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
    """
    Fetches arrival data for **all** stations concurrently.
    This is useful for a dashboard or overview, but may be slow.
    """
    all_data = {}

    async with httpx.AsyncClient() as client:
        # Create a list of tasks to run in parallel
        tasks = []
        station_names = list(STATION_IDS.keys())
        for station_name in station_names:
            station_id = STATION_IDS[station_name]
            tasks.append(fetch_train_data(client, station_id))

        # Run all tasks concurrently
        results = await asyncio.gather(*tasks)

        # Map results back to station names
        for station_name, data in zip(station_names, results):
            if not data.get("error"):
                all_data[station_name] = data.get("all_departures", {})

    return all_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8085)