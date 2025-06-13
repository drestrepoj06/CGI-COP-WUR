# Build frontend
FROM node:20-bullseye as frontend-builder
WORKDIR /src/frontend
COPY frontend/package.json ./
COPY frontend/ ./
RUN npm install && npm run build

# Python backend
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
COPY --from=frontend-builder /src/frontend-dist ./frontend-dist
EXPOSE 5000
CMD ["python", "api_server.py"]