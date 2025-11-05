# Save this as train_scraper.py
import httpx
from bs4 import BeautifulSoup

# --- Station IDs (Data) ---
STATION_IDS = {
    "Retiro": 75,
    "Saldias": 78,
    "Ciudad Universitaria": 80,
    "A. del Valle": 82,
    "Padilla": 84,
    "Florida": 86,
    "Munro": 88,
    "Carapachay": 130,
    "Villa Adelina": 90,
    "Boulogne Sur Mer": 95,
    "A. Montes": 97,
    "Don Torcuato": 100,
    "A. Sordeaux": 103,
    "Villa de Mayo": 105,
    "Los Polvorines": 108,
    "Pablo Nogues": 111,
    "Grand Bourg": 113,
    "Tierras Altas": 116,
    "Tortuguitas": 118,
    "M. Alberti": 120,
    "Del Viso": 123,
    "Cecilia Grierson": 135,
    "Villa Rosa": 126
}

async def fetch_train_data(client: httpx.AsyncClient, station_id: int) -> dict:
    """
    Asynchronously fetches train departure data for a given station ID.

    Args:
        client: An httpx.AsyncClient instance.
        station_id: The integer ID of the station.

    Returns:
        A dictionary of departure data or an error dictionary.
    """
    url = "http://proximostrenes.ferrovias.com.ar/estaciones.asp"
    payload = {"idEst": station_id, "adm": 1}

    try:
        response = await client.post(url, data=payload, timeout=10.0)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("table#table_main_box table#table_main tr")

        departures = {}
        for row in rows:
            destination_cell = row.select_one("td.tdEst")
            time_cell = row.select_one("td.tdEst.tdEstr.tdflecha")

            if destination_cell and time_cell:
                destination = destination_cell.get_text(strip=True)
                time = time_cell.get_text(strip=True)

                if destination and time and "nbsp" not in destination:
                    if destination not in departures:
                        departures[destination] = []
                    departures[destination].append(time)

        if not departures:
            return {"message": "No departure times found."}

        # --- Split data by direction, similar to your Swift logic ---
        retiro_departures = {}
        villarosa_departures = {} # For all non-Retiro destinations

        for dest, times in departures.items():
            if "RETIRO" in dest.upper():
                retiro_departures[dest] = times
            else:
                villarosa_departures[dest] = times

        return {
            "all_departures": departures,
            "to_retiro": retiro_departures,
            "to_villa_rosa_branch": villarosa_departures
        }

    except httpx.RequestError as e:
        return {"error": f"Network error: {e}"}
    except Exception as e:
        return {"error": f"Parsing error: {e}"}