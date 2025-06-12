FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .



# CMD ["python", "ingestion.py"]

ENTRYPOINT ["bash", "-c", "streamlit run web.py & python ingestion.py & wait"]
