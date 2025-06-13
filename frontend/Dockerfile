FROM node:18

WORKDIR /app
COPY package.json package-lock.json .

RUN rm -rf node_modules package-lock.json && \
    npm install && \
    npm install @vitejs/plugin-vue --save-dev && \
    npm install leaflet && \
    npm install chart.js


COPY . .

EXPOSE 8080
CMD ["npm", "run", "dev", "--", "--port", "8080", "--host"]
