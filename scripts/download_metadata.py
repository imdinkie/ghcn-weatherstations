import requests

urls = {
    "stations": "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt",
    "inventory": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
}

for name, url in urls.items():
    print(f"Downloading {name} metadata...")
    response = requests.get(url)
    response.raise_for_status()
    with open(f"data/{name}.txt", "wb") as f:
        f.write(response.content)
    print(f"Saved {name} metadata to data/{name}.txt")