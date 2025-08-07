import json
import csv
from geopy.distance import geodesic
import unidecode

# Define input and output file paths
INPUT_JSON = "validators.json"  
OUTPUT_CSV = "validators_with_metro.csv"

# Manual mapping: Cities explicitly assigned to metro areas
MANUAL_METRO_MAP = {
    "amsterdam": "Amsterdam Metropolitan Area",
    "anchorage": "Anchorage Metropolitan Area",
    "arezzo": "Florence Metropolitan Area",
    "ashburn": "Washington DC Metropolitan Area",
    "atlanta": "Atlanta Metropolitan Area",
    "baton rouge": "Baton Rouge Metropolitan Area",
    "beauharnois": "Montreal Metropolitan Area",
    "bend": "Portland Metropolitan Area",
    "bialystok": "Bialystok Metropolitan Area",
    "bogotá": "Bogotá Metropolitan Area",
    "bogota": "Bogota Metropolitan Area",
    "boston": "Boston Metropolitan Area",
    "brașov": "Brașov Metropolitan Area",
    "bucharest": "Bucharest Metropolitan Area",
    "buenos aires": "Buenos Aires Metropolitan Area",
    "charlotte": "Charlotte Metropolitan Area",
    "chelyabinsk": "Chelyabinsk Metropolitan Area",
    "columbus": "Columbus Metropolitan Area",
    "coventry": "West Midlands Metropolitan Area",
    "dallas": "Dallas Metropolitan Area",
    "denver": "Denver Metropolitan Area",
    "draper": "Salt Lake City Metropolitan Area",
    "dublin": "Dublin Metropolitan Area",
    "duivendrecht": "Amsterdam Metropolitan Area",
    "düsseldorf": "Düsseldorf Metropolitan Area",
    "espoo": "Helsinki Metropolitan Area",
    "essen": "Ruhr Metropolitan Area",
    "fort wayne": "Fort Wayne Metropolitan Area",
    "fort worth": "Dallas-Fort Worth Metroplex",
    "frankfurt am main": "Frankfurt Metropolitan Area",
    "frankfurt": "Frankfurt Metropolitan Area",
    "haarlem": "Amsterdam Metropolitan Area",
    "hamburg": "Hamburg Metropolitan Region",
    "helsinki": "Helsinki Metropolitan Area",
    "isando": "Johannesburg Metropolitan Area",
    "johannesburg": "Johannesburg Metropolitan Area",
    "kazan": "Kazan Metropolitan Area",
    "kempton park": "Johannesburg Metropolitan Area",
    "kent": "Seattle Metropolitan Area",
    "laredo": "Laredo-Nuevo Laredo Metro",
    "las vegas": "Las Vegas Metropolitan Area",
    "limburg": "Limburg Metropolitan Area",
    "linden": "New York Metropolitan Area",
    "luxembourg city": "Luxembourg Metropolitan Area",
    "lviv": "Lviv Metropolitan Area",
    "manchester": "Manchester Metropolitan Area",
    "mayfield": "Cleveland Metro Area",
    "melbourne": "Melbourne Metropolitan Area",
    "montreal": "Montreal Metropolitan Area",
    "moscow": "Moscow Metropolitan Area",
    "münster": "Münster Metropolitan Area",
    "munster": "Munster Metropolitan Area",
    "mykolaiv": "Mykolaiv Metropolitan Area",
    "novosibirsk": "Novosibirsk Metropolitan Area",
    "nuremberg": "Nuremberg Metropolitan Area",
    "offenbach": "Frankfurt Metropolitan Area",
    "ogden": "Salt Lake City Metropolitan Area",
    "osaka": "Osaka Metropolitan Area",
    "oslo": "Oslo Metropolitan Area",
    "ottobrunn": "Munich Metropolitan Area",
    "philadelphia": "Philadelphia Metropolitan Area",
    "pittsburgh": "Pittsburgh Metropolitan Area",
    "podolsk": "Moscow Metropolitan Area",
    "portland": "Portland Metropolitan Area",
    "prague": "Prague Metropolitan Area",
    "pushkin": "Saint Petersburg Metropolitan Area",
    "querétaro": "Querétaro Metropolitan Area",
    "queretaro": "Queretaro Metropolitan Area",
    "radom": "Radom Metropolitan Area",
    "resita": "Romania Metropolitan Area",
    "reston": "Washington DC Metropolitan Area",
    "riga": "Riga Metropolitan Area",
    "roosendaal": "Rotterdam Metropolitan Area",
    "rotterdam": "Rotterdam Metropolitan Area",
    "saint petersburg": "Saint Petersburg Metropolitan Area",
    "salt lake city": "Salt Lake City Metropolitan Area",
    "santiago": "Santiago Metropolitan Area",
    "são paulo": "São Paulo Metropolitan Area",
    "sendai": "Sendai Metropolitan Area",
    "sheffield": "Sheffield Metropolitan Area",
    "šiauliai": "Lithuania Metropolitan Area",
    "siauliai": "Lithuania Metropolitan Area",
    "singapore": "Singapore Metropolitan Area",
    "sofia": "Sofia Metropolitan Area",
    "spånga": "Stockholm Metropolitan Area",
    "spanga": "Stockholm Metropolitan Area",
    "st. louis": "St. Louis Metropolitan Area",
    "stirling": "Glasgow Metropolitan Area",
    "stockholm": "Stockholm Metropolitan Area",
    "tampa": "Tampa Bay Area",
    "tirana": "Tirana Metropolitan Area",
    "toronto": "Greater Toronto Area",
    "vancouver": "Metro Vancouver",
    "vienna": "Vienna Metropolitan Area",
    "vilnius": "Vilnius Metropolitan Area",
    "waldbrunn": "Germany Metropolitan Area",
    "warsaw": "Warsaw Metropolitan Area",
    "washington": "Washington DC Metropolitan Area",
    "wilmington": "Philadelphia Metropolitan Area",
    "yekaterinburg": "Yekaterinburg Metropolitan Area",
    "żabno": "Poland Metropolitan Area",
    "zabno": "Poland Metropolitan Area",
    "zgierz": "Lodz Metropolitan Area",
}

# Function to normalize city names
def normalize_city(city):
    if not city:
        return None
    return unidecode.unidecode(city.lower().strip())

# Load JSON data
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract metro areas dynamically based on lat/lon
metro_locations = {}
for entry in data:
    city = entry.get("ip_city")
    lat, lon = entry.get("ip_latitude"), entry.get("ip_longitude")

    if city and lat and lon:
        normalized_city = normalize_city(city)
        if normalized_city not in MANUAL_METRO_MAP:
            metro_locations[normalized_city] = (float(lat), float(lon))

# Function to find the nearest metro area dynamically
def find_nearest_metro(lat, lon):
    min_distance = float("inf")
    nearest_metro = "Unknown Metro Area"
    
    for metro, coords in metro_locations.items():
        distance = geodesic((lat, lon), coords).km
        if distance < min_distance and distance < 100:  # 100 km threshold
            min_distance = distance
            nearest_metro = metro.title() + " Metro"
    
    return nearest_metro

# Map cities to metro areas
city_metro_list = []
seen_cities = set()

for entry in data:
    city = entry.get("ip_city")
    lat, lon = entry.get("ip_latitude"), entry.get("ip_longitude")

    if city and isinstance(city, str):
        normalized_city = normalize_city(city)
        
        # Use manual mapping if available
        metro_area = MANUAL_METRO_MAP.get(normalized_city)
        
        # If no manual mapping, find nearest metro
        if not metro_area and lat and lon:
            metro_area = find_nearest_metro(float(lat), float(lon))
        
        if not metro_area:
            metro_area = "Unknown Metro Area"

        if city not in seen_cities:
            city_metro_list.append([city, metro_area])
            seen_cities.add(city)

# Write to CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["ip_city", "metro_area"])
    writer.writerows(city_metro_list)

print(f"CSV file '{OUTPUT_CSV}' created successfully with {len(city_metro_list)} unique city entries.")
