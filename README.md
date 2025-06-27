1. change train/ambu icons in geofence area.
2. solve problems with create incident button and resolve button. (Falco, done)
3. Severity of geofence rail segment and assign different colors based on that.
4. Put the scale bar in the map (Thijs, done)
5. Add estimated resolving time, incident info (train index). (Xiaolu, done)
6. Release the ambus after resolve.
7. Add function that iterates through keys from tomtom. (Falco, done)
8. Add a virtual clock based on average time of the train and ambulance. (Thijs)



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
