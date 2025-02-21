# mtg-database

Self-hosted Postgres database implementation for MTG.

### 1. Set .env values to init the db server

```.env
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword
POSTGRES_DB=mtgdb
```


### 1. Build the Docker Image

```bash
docker build -t mtg-postgres .
```

2. Run the Container Using the .env File
#### For Linux/macOS:

docker run -d \
  --name my-postgres \
  --env-file .env \
  -p 5432:5432 \
  -v ./postgres:/var/lib/postgresql/data \
  mtg-postgres

#### For Windows CMD:

docker run -d --name mtg-postgres --env-file .env -p 5432:5432 -v %cd%\postgres:/var/lib/postgresql/data mtg-postgres

#### For Windows PowerShell:

docker run -d --name mtg-postgres --env-file .env -p 5432:5432 -v ${PWD}\postgres