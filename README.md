- [ ] Not urgent: Write doc to explain mock-up data file:  
- [ ] 1a. Add random train selector, change this train's FIELD 'status' from TRUE to FALSE, write the current location to FIELD 'accident_location'
- [ ] 1b. Change 1km nearby railsegments FIELD 'status' from TRUE to FALSE
- [ ] 2a. **Calculate estimated driving time from current ambu to this 'accident_location' based on Tomtom API, change this ambu's FIELD 'status' from TRUE to FALSE. Write the travel path of this ambu to Tile38 DB according to timestamp.**
- [ ] 2b. Create a Popup window to notify the accident.
- [ ] 2c. Send **GEOFENCING** on the segments that 'status' are FALSE. Send alert when other trains come across them.
Above should be finished before Wed meeting.

- [ ] 3a. Update pie chart.
- [ ] 4a. Assign color to different road based on traffic density.
- [ ] 4b. Increase speed for ambu when it's on it's duty.
- [ ] 4c. A progress bar to reveal the time ambu takes to arrive the 'accident_location'.
- [ ] 5a. What happen when we want more ambu for the same 'accident_location'.


<div align="center">
  <img src="resources/rcop-logo.png" width="600"/>
</div>

------

<div align="center">
  <img src="https://avatars.githubusercontent.com/drestrepoj06" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/fyan1024" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/ThijsVons" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/FalcoWolf1212" width="50" style="border-radius:50%"/>
  <img src="https://avatars.githubusercontent.com/matetorok1" width="50" style="border-radius:50%"/>
</div>
Apparently Máté, John, Xiaolu are working on the backend, while Falco and Thijs are working on the frontend.



## Introduction
RCOP is an open source Rail Common Operation Picture toolbox based on a series of open source data-handling tools. You can also consider this a minimal integration of traffic monitoring dashboard built on [Apache Kafka](https://kafka.apache.org/), [Pyle38](https://github.com/iwpnd/pyle38) (Tile38), [Streamlit](https://streamlit.io/), [Plotly](https://plotly.com/python/). 



# CGI-COP-WUR
>Repository for the CGI COP project
