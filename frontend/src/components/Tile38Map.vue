<template>
    <div id="map" style="height: 500px;"></div>
  </template>
  
  <script setup>
  import { onMounted } from "vue";
  import L from "leaflet";
  
  const props = defineProps(["trainData", "ambulanceData"]);
  
  onMounted(() => {
    const map = L.map("map").setView([52.1, 5.1], 12);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "© OpenStreetMap contributors"
    }).addTo(map);

    props.trainData.forEach(train => {
      L.circleMarker([train.lat, train.lon], { radius: 6, color: "blue" })
        .bindPopup(`Train ${train.ritId} - Speed: ${train.info.speed} km/h`)
        .addTo(map);
    });

    props.ambulanceData.forEach(ambulance => {
      L.circleMarker([ambulance.lat, ambulance.lon], { radius: 6, color: "red" })
        .bindPopup(`Ambulance ${ambulance.vehicle_number} - Status: ${ambulance.info.status}`)
        .addTo(map);
    });
  });


  </script>
  