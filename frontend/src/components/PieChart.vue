<template>
    <div>
      <canvas ref="chartCanvas"></canvas>
    </div>
  </template>
  
  <script setup>
  import { onMounted, ref } from "vue";
  import Chart from "chart.js/auto";
  
  const props = defineProps(["ambulanceStatus"]);
  const chartCanvas = ref(null);
  
  onMounted(() => {
    new Chart(chartCanvas.value, {
      type: "doughnut",
      data: {
        labels: ["Available", "In Use"],
        datasets: [{
          data: props.ambulanceStatus,
          backgroundColor: ["#27AE60", "#E74C3C"]
        }]
      }
    });
  });
  </script>
  