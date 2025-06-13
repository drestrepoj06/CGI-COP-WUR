<template>
  <div class="dashboard">
    <aside>
      <h2>🚑 Ambulance Station Availability</h2>
      <table>
        <thead>
          <tr>
            <th>Station</th>
            <th>Available</th>
            <th>Capacity</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in stations" :key="row.Station">
            <td>{{ row.Station }}</td>
            <td>
              <span :class="['pill', row.Ambulances > 0 ? 'pill-green' : 'pill-red']">
                {{ row.Ambulances }}
              </span>
            </td>
            <td>{{ row.Capacity }}</td>
          </tr>
        </tbody>
      </table>
    </aside>
    <main>
      <div class="map-panel">
        <h1>COP Dashboard</h1>
        <MapDisplay :trains="trains" :ambulances="ambulances" />
      </div>
    </main>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import axios from 'axios'
import MapDisplay from './components/MapDisplay.vue'

const stations = ref([
  { Station: "Maarssen", Ambulances: 3, Capacity: 5 },
  { Station: "Vader Rijndreef", Ambulances: 5, Capacity: 6 },
  { Station: "Diakonessenhuis", Ambulances: 2, Capacity: 4 },
  { Station: "On the move", Ambulances: 5, Capacity: 8 }
])

const ambulances = ref([])
const trains = ref([])

const fetchData = async () => {
  const [trainRes, ambRes] = await Promise.all([
    axios.get('/api/trains'),
    axios.get('/api/ambulances')
  ])
  trains.value = trainRes.data
  ambulances.value = ambRes.data
}

onMounted(() => {
  fetchData()
  setInterval(fetchData, 5000)
})
</script>

<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

:root {
  --bg-dark: #191c24;
  --card: #23263a;
  --accent: #22d47b;
  --danger: #ff4d4d;
  --text-primary: #f1f2f6;
  --text-secondary: #b0b2be;
}

body {
  font-family: 'Inter', Arial, sans-serif;
  background: var(--bg-dark);
  color: var(--text-primary);
}

.dashboard {
  display: grid;
  grid-template-columns: 400px 1fr;
  gap: 2rem;
  height: 100vh;
  background: var(--bg-dark);
}

aside {
  background: var(--card);
  border-radius: 20px;
  padding: 2rem 1.5rem;
  box-shadow: 0 4px 24px rgba(0,0,0,0.15);
  display: flex;
  flex-direction: column;
  min-width: 260px;
}

aside h2 {
  color: var(--accent);
  font-size: 1.1rem;
  margin-bottom: 1rem;
  letter-spacing: 0.02em;
}

table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0 8px;
  margin-bottom: 1.8rem;
  color: var(--text-primary);
  font-size: 1rem;
}

th, td {
  text-align: left;
  padding: 6px 10px;
}

th {
  color: var(--text-secondary);
  font-weight: 600;
  font-size: 0.95rem;
}

tr {
  background: #24263e;
  border-radius: 8px;
}

.pill {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 999px;
  font-weight: bold;
  font-size: 1em;
  color: #fff;
}

.pill-green { background: var(--accent); }
.pill-red { background: var(--danger); }

main {
  display: flex;
  align-items: flex-start;
  justify-content: stretch;
  flex-direction: column;
  padding: 2rem 1.5rem 2rem 0;
  min-width: 600px;
}

.map-panel {
  background: var(--card);
  border-radius: 18px;
  padding: 1.5rem 1.5rem 1rem 1.5rem;
  box-shadow: 0 4px 24px rgba(0,0,0,0.10);
  width: 100%;
  height: 99%;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.map-panel h1 {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--accent);
  margin-bottom: 0.5rem;
  letter-spacing: 0.03em;
}

::-webkit-scrollbar {
  width: 10px;
  background: #23263a;
  border-radius: 8px;
}
::-webkit-scrollbar-thumb {
  background: #191c24;
  border-radius: 8px;
}
</style>