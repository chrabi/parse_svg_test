import React, { useState } from 'react';
import { Card, CardContent } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Input } from '@/components/ui/input';

// Przykładowe dane
const MOCK_DATA = {
  floors: [
    {
      id: 1,
      name: "Piętro 1",
      racks: [
        { id: 1, x: 50, y: 50, width: 60, height: 120, name: "RACK-A1", servers: generateServers(15) },
        { id: 2, x: 150, y: 50, width: 60, height: 120, name: "RACK-A2", servers: generateServers(18) },
        { id: 3, x: 250, y: 50, width: 60, height: 120, name: "RACK-A3", servers: generateServers(12) }
      ]
    },
    {
      id: 2,
      name: "Piętro 2",
      racks: [
        { id: 4, x: 50, y: 50, width: 60, height: 120, name: "RACK-B1", servers: generateServers(20) },
        { id: 5, x: 150, y: 50, width: 60, height: 120, name: "RACK-B2", servers: generateServers(16) }
      ]
    }
  ]
};

// Funkcja generująca przykładowe serwery
function generateServers(count) {
  return Array.from({ length: count }, (_, i) => ({
    id: `SRV-${i + 1}`,
    name: `Server-${i + 1}`,
    powerUsage: Math.round(Math.random() * 1000),
    cpuUsage: Math.round(Math.random() * 100),
    ramUsage: Math.round(Math.random() * 100),
    totalRam: 128,
    position: i + 1
  }));
}

const DataCenterFloorPlan = () => {
  const [selectedFloor, setSelectedFloor] = useState(MOCK_DATA.floors[0]);
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedRack, setSelectedRack] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Filtrowanie szaf po wyszukiwaniu
  const filteredRacks = selectedFloor.racks.filter(rack => 
    rack.servers.some(server => 
      server.name.toLowerCase().includes(searchQuery.toLowerCase())
    )
  );

  const handleRackClick = (rack) => {
    setSelectedRack(rack);
    setIsModalOpen(true);
  };

  return (
    <div className="p-4 space-y-4">
      <div className="flex space-x-4 mb-4">
        <Select 
          value={selectedFloor.id.toString()} 
          onValueChange={(value) => setSelectedFloor(MOCK_DATA.floors.find(f => f.id.toString() === value))}
        >
          <SelectTrigger className="w-48">
            <SelectValue placeholder="Wybierz piętro" />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {MOCK_DATA.floors.map(floor => (
                <SelectItem key={floor.id} value={floor.id.toString()}>
                  {floor.name}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>

        <Input
          placeholder="Szukaj serwera..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-64"
        />
      </div>

      <Card>
        <CardContent className="p-6">
          <svg width="800" height="600" viewBox="0 0 800 600">
            <rect width="800" height="600" fill="#f0f0f0" />
            {filteredRacks.map(rack => (
              <g key={rack.id} onClick={() => handleRackClick(rack)} className="cursor-pointer">
                <rect
                  x={rack.x}
                  y={rack.y}
                  width={rack.width}
                  height={rack.height}
                  fill={searchQuery && rack.servers.some(s => s.name.toLowerCase().includes(searchQuery.toLowerCase())) 
                    ? "#90cdf4" 
                    : "#4a5568"}
                  stroke="#2d3748"
                  strokeWidth="2"
                />
                <text
                  x={rack.x + rack.width / 2}
                  y={rack.y + rack.height + 20}
                  textAnchor="middle"
                  fill="#2d3748"
                  className="text-sm"
                >
                  {rack.name}
                </text>
              </g>
            ))}
          </svg>
        </CardContent>
      </Card>

      <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <DialogTitle>Szczegóły szafy {selectedRack?.name}</DialogTitle>
          </DialogHeader>
          
          <div className="space-y-4">
            <svg width="400" height="600" viewBox="0 0 400 600">
              <rect width="400" height="600" fill="#f0f0f0" />
              {selectedRack?.servers.map((server, index) => (
                <g key={server.id}>
                  <rect
                    x={50}
                    y={50 + index * 25}
                    width={300}
                    height={20}
                    fill="#4a5568"
                    stroke="#2d3748"
                  />
                  <text
                    x={200}
                    y={65 + index * 25}
                    textAnchor="middle"
                    fill="white"
                    className="text-xs"
                  >
                    {server.name}
                  </text>
                </g>
              ))}
            </svg>

            <div className="w-full overflow-x-auto">
              <table className="min-w-full bg-white">
                <thead className="bg-gray-100">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Nazwa</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Pozycja</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Zużycie prądu (W)</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">CPU (%)</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">RAM (%)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {selectedRack?.servers.slice(0, 20).map(server => (
                    <tr key={server.id} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{server.name}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{server.position}U</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{server.powerUsage}W</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{server.cpuUsage}%</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{server.ramUsage}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default DataCenterFloorPlan;
