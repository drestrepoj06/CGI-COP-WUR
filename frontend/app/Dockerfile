FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY animated_map.html .
COPY data/train_location_data/ ./train_location_data/

COPY . .

CMD ["streamlit", "run", "main.py"]