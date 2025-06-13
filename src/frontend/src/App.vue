<template>
  <div class="dashboard">
    <aside>
      <h2>Ambulance station availability</h2>
      <table>
        <tr>
          <th>Station</th>
          <th>Available</th>
          <th>Capacity</th>
        </tr>
        <tr v-for="row in stations" :key="row.Station">
          <td>{{ row.Station }}</td>
          <td>{{ row.Ambulances }}</td>
          <td>{{ row.Capacity }}</td>
        </tr>
      </table>
      <h2>Ambulance availability</h2>
      <div class="donuts">
        <canvas v-for="(row, i) in stations" :key="'donut'+i" :ref="'donut'+i"></canvas>
      </div>
    </aside>
    <main>
      <MapDisplay :trains="trains" :ambulances="ambulances" />
    </main>
    <nav>
      <button>Button 1</button>
      <button>Button 2</button>
      <button>Button 3</button>
    </nav>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import axios from 'axios'
import MapDisplay from './components/MapDisplay.vue'
import Chart from 'chart.js/auto'

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

const drawDonuts = () => {
  stations.value.forEach((row, i) => {
    const el = document.querySelector(`canvas[ref="donut${i}"]`)
    if (el) {
      new Chart(el.getContext('2d'), {
        type: 'doughnut',
        data: {
          labels: ['Available', 'In Use'],
          datasets: [{
            data: [row.Ambulances, row.Capacity - row.Ambulances],
            backgroundColor: ['#27ae60', '#e74c3c']
          }]
        },
        options: { cutout: '70%', plugins: { legend: { display: false } } }
      })
    }
  })
}

onMounted(drawDonuts)
watch(stations, drawDonuts)
</script>

<style>
.dashboard {
  display: grid;
  grid-template-columns: 1fr 2.5fr 0.5fr;
  gap: 1rem;
  height: 100vh;
}
aside {
  padding: 1rem;
  background: #f5f5f5;
}
main {
  display: flex;
  align-items: stretch;
  justify-content: stretch;
}
nav {
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  gap: 1rem;
  padding: 1rem;
}
.donuts {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}
</style>