FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY frontend/animated_map.html .
COPY frontend/data/train_location_data/ ./train_location_data/
COPY frontend/data/ambulance_location_data/ ./ambulance_location_data/



# CMD ["python", "backend/main.py"]
ENTRYPOINT ["bash", "-c", "streamlit run frontend/web.py & python backend/main.py & wait"]
# manually open the browser for http://localhost:8501/


