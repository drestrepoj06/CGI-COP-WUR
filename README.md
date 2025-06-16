- [ ] Not urgent: Write doc to explain mock-up data file:  
- [x] 1a. Add random train selector, change this train's FIELD 'status' from TRUE to FALSE, write the current location to TOPIC 'broken_train'
- [ ] 2a. **Calculate estimated driving time from current ambu to this 'accident_location' based on Tomtom API, change this ambu's FIELD 'status' from TRUE to FALSE. Write the travel path of this ambu to Tile38 DB according to timestamp.**
- [ ] 2b. Create a Popup window to notify the accident.
- [ ] 2c. Send **GEOFENCING** on the segments that 'status' are FALSE. Send alert when other trains come across them.
- [ ] 2d. Change 1km nearby railsegments FIELD 'status' from TRUE to FALSE lay them over real-time traffic density
- [ ] 2e. Add railway to map.

Above should be finished before Wed meeting.

---

- [ ] 3a. Update pie chart.
- [ ] 4a. Assign color to different road based on traffic density.
- [ ] 4b. Increase speed for ambu when it's on it's duty.
- [ ] 4c. A progress bar to reveal the time ambu takes to arrive the 'accident_location'.
- [ ] 5a. What happen when we want more ambu for the same 'accident_location'.

---

# 🚆 RCOP ![Python](https://img.shields.io/badge/Python-3.12-green?logo=python)![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)![License](https://img.shields.io/badge/License-GPL-green)

<div align="center">
  <img src="resources/rcop-logo.png" width="600"/>
</div>

Welcome to **RCOP**, a powerful tool for monitoring the accident response operations at Utrecht Centraal Railway Station in simulation.

RCOP is an open source Rail Common Operation Picture toolbox based on a series of open source data-handling tools. You can also consider this a minimal integration of traffic monitoring dashboard built on [Apache Kafka](https://kafka.apache.org/), [TIle38](https://tile38.com/), [Streamlit](https://streamlit.io/), [Mapbox](https://www.mapbox.com/). 

## 🛠 Installation


Follow these simple steps to set up the system:

1. Clone the repository:
  ```bash
  git https://github.com/drestrepoj06/CGI-COP-WUR.git
  ```
2. Navigate to the project folder:
  ```bash
  cd CGI-COP-WUR
  ```
3. Launch the service using Docker:
  ```bash
  docker compose up
  ```

🚀 Then open localhost:8501 to start monitoring the lovely Railway Station **Utrecht Centraal**.

## 🤝 Contributors

Xiaolu Yan, Jhon Restrepo, Falco Latour, Máté Török, Thijs Vons​

<div align="center">
  <img src="https://avatars.githubusercontent.com/drestrepoj06" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/fyan1024" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/ThijsVons" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/FalcoWolf1212" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/matetorok1" width="50" style="border-radius:50%"/>
</div>

The RCOP project was made possible with the help of many:

- `Robert Voûte`, `Albert Jan van der Werp`, `Hessel Prins` from CGI
- `Sytze de Bruin`, `Jascha Grübel` from WUR-GRS
