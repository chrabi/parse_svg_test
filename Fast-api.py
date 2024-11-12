from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Dodanie CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modele danych
class Server(BaseModel):
    id: str
    name: str
    powerUsage: float
    cpuUsage: float
    ramUsage: float
    totalRam: int
    position: int

class Rack(BaseModel):
    id: int
    x: int
    y: int
    width: int
    height: int
    name: str
    servers: List[Server]

class Floor(BaseModel):
    id: int
    name: str
    racks: List[Rack]

# Przykładowe dane
MOCK_DATA = {
    "floors": [
        {
            "id": 1,
            "name": "Piętro 1",
            "racks": [
                {
                    "id": 1,
                    "x": 50,
                    "y": 50,
                    "width": 60,
                    "height": 120,
                    "name": "RACK-A1",
                    "servers": [
                        {
                            "id": f"SRV-{i}",
                            "name": f"Server-{i}",
                            "powerUsage": 500,
                            "cpuUsage": 75,
                            "ramUsage": 80,
                            "totalRam": 128,
                            "position": i
                        } for i in range(1, 16)
                    ]
                },
                # Dodaj więcej szaf...
            ]
        },
        # Dodaj więcej pięter...
    ]
}

@app.get("/floors", response_model=List[Floor])
async def get_floors():
    return MOCK_DATA["floors"]

@app.get("/floors/{floor_id}", response_model=Floor)
async def get_floor(floor_id: int):
    floor = next((f for f in MOCK_DATA["floors"] if f["id"] == floor_id), None)
    if not floor:
        raise HTTPException(status_code=404, detail="Floor not found")
    return floor

@app.get("/racks/{rack_id}", response_model=Rack)
async def get_rack(rack_id: int):
    for floor in MOCK_DATA["floors"]:
        for rack in floor["racks"]:
            if rack["id"] == rack_id:
                return rack
    raise HTTPException(status_code=404, detail="Rack not found")

@app.get("/search/servers")
async def search_servers(query: str):
    results = []
    for floor in MOCK_DATA["floors"]:
        for rack in floor["racks"]:
            for server in rack["servers"]:
                if query.lower() in server["name"].lower():
                    results.append({
                        "server": server,
                        "rack": rack["name"],
                        "floor": floor["name"]
                    })
    return results

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
