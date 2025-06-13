<template>
    <div class="dashboard">
      <Tile38Map :trainData="trainData" :ambulanceData="ambulanceData" />
      <PieChart :ambulanceStatus="ambulanceStatus" />
    </div>
  </template>
  
  <script setup>
  import { ref, onMounted } from "vue";
  import Tile38Map from "./Tile38Map.vue";
  import PieChart from "./PieChart.vue";
  
  const trainData = ref([]);
  const ambulanceData = ref([]);
  const ambulanceStatus = ref([]);


  const fetchTrainData = async () => {
    const response = await fetch("http://localhost:5000/api/train");
    const data = await response.json();

    trainData.value = data[1].map(train => ({
      ritId: train[0],  // ID
      lat: train[1].coordinates[1],  // Latitude
      lon: train[1].coordinates[0],  // Longitude
      info: JSON.parse(train[2][1])  // Train details
    }));
  };

  const fetchAmbulanceData = async () => {
    const response = await fetch("http://localhost:5000/api/ambulance");
    const data = await response.json();

    ambulanceData.value = data[1].map(ambulance => ({
      vehicle_number: ambulance[0],  // ID
      lat: ambulance[1].coordinates[1],  // Latitude
      lon: ambulance[1].coordinates[0],  // Longitude
      info: JSON.parse(ambulance[2][1])  // Ambulance details
    }));
  };


  
  onMounted(() => {
    fetchTrainData();
    fetchAmbulanceData();
  });
  </script>
  