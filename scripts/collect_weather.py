import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv('OPENWEATHER_API_KEY')
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

# List of European cities to analyze
CITIES = [
    'Paris', 'London', 'Berlin', 'Madrid', 'Rome',
    'Amsterdam', 'Brussels', 'Vienna', 'Prague', 'Lisbon',
    'Stockholm', 'Copenhagen', 'Dublin', 'Athens', 'Warsaw'
]


def get_weather(city, api_key):
    """Fetch weather data for a specific city"""
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Extract relevant data
        weather_info = {
            'city': data['name'],
            'country': data['sys']['country'],
            'temperature': round(data['main']['temp'], 2),
            'feels_like': round(data['main']['feels_like'], 2),
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'weather': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'wind_speed': round(data['wind']['speed'], 2),
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon'],
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

        print(f"OK - {city}: {weather_info['temperature']}C")
        return weather_info

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"ERROR - {city}: Rate limit exceeded, wait a bit...")
        else:
            print(f"ERROR - {city}: HTTP {e.response.status_code}")
        return None
    except Exception as e:
        print(f"ERROR - {city}: {e}")
        return None


def main():
    print("=" * 50)
    print("Weather Data Collection")
    print("=" * 50)

    # Check if API key exists
    if not API_KEY or API_KEY == 'your_api_key_here':
        print("ERROR: Missing API key!")
        print("-> Create .env file with: OPENWEATHER_API_KEY=your_key")
        print("-> Get free key at: https://openweathermap.org/api")
        return

    print(f"API Key: {API_KEY[:8]}...")
    print(f"Cities to process: {len(CITIES)}\n")

    weather_data = []

    # Collect data for each city
    for i, city in enumerate(CITIES, 1):
        print(f"[{i}/{len(CITIES)}] ", end='')
        data = get_weather(city, API_KEY)

        if data:
            weather_data.append(data)

        # Wait 1 second between calls (API limit: 60/min)
        if i < len(CITIES):
            time.sleep(1)

    if not weather_data:
        print("\nERROR: No data collected!")
        return

    # Save to JSON
    output_file = 'data/input/weather_data.json'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 50)
    print(f"SUCCESS: Data saved to {output_file}")
    print(f"Cities collected: {len(weather_data)}/{len(CITIES)}")

    # Quick stats
    temps = [d['temperature'] for d in weather_data]
    print(f"\nAverage temperature: {sum(temps)/len(temps):.1f}C")
    print(f"Hottest: {max(temps):.1f}C")
    print(f"Coldest: {min(temps):.1f}C")
    print("=" * 50)


if __name__ == "__main__":
    main()
