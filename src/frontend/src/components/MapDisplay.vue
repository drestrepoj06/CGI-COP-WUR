<template>
  <div id="map" style="height: 500px; width: 100%;"></div>
</template>

<script setup>
import { onMounted, watch } from 'vue'
import L from 'leaflet'

const props = defineProps({
  trains: Array,
  ambulances: Array
})

let map
let trainMarkers = {}
let ambulanceMarkers = {}

function updateMarkers(collection, markers, color, latKey, lonKey, idKey) {
  collection.forEach(item => {
    const id = item[idKey]
    const latlng = [item[latKey], item[lonKey]]
    if (!markers[id]) {
      markers[id] = L.circleMarker(latlng, {
        radius: 6,
        color,
        fillColor: color,
        fillOpacity: 0.8
      }).addTo(map).bindPopup(`${idKey === 'ritId' ? 'Train' : 'Ambulance'} ${id}`)
    } else {
      markers[id].setLatLng(latlng)
    }
  })
}

onMounted(() => {
  map = L.map('map').setView([52.1, 5.1], 12)
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap contributors'
  }).addTo(map)
  L.circle([52.094040, 5.093102], {
    radius: 10000,
    color: 'black',
    fillColor: '#f03',
    fillOpacity: 0
  }).addTo(map)
})

watch(() => props.trains, (trains) => {
  if (!map) return
  updateMarkers(trains, trainMarkers, 'blue', 'lat', 'lon', 'ritId')
})

watch(() => props.ambulances, (ambulances) => {
  if (!map) return
  updateMarkers(ambulances, ambulanceMarkers, 'red', 'lat', 'lon', 'vehicle_number')
})
</script>