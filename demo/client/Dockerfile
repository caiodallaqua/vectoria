FROM node:18-bullseye AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM node:18-bullseye AS final
WORKDIR /app
COPY --from=build /app ./
CMD ["npm", "run", "preview", "--", "--host", "--port", "3000"]
