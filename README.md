# ETL Pipeline – Local Docker Setup

## Prerequisites

- Install Docker Desktop
- Have a modern web browser (Chrome, Edge, Firefox, etc.)

---

## Verify Docker Installation

```bash
docker --version
```

---

## Launch the Containers

```bash
docker compose up -d
```

### Command Reference

- `docker` – Docker CLI
- `compose` – Uses `docker-compose.yml`
- `up` – Builds and starts containers
- `-d` – Runs containers in detached (background) mode

This command builds and starts the following services:

- JupyterLab
- PostgreSQL
- pgAdmin

---

## Access JupyterLab

Open a browser and navigate to:

```
http://localhost:8888
```

If prompted for a token (should not), retrieve it with:

```bash
docker compose logs jupyter
```

Copy the URL containing the token and open it in your browser.

---

## Access pgAdmin

Open a browser and navigate to:

```
http://localhost:5050
```

### Login Credentials

- Email: `admin@example.com`
- Password: `admin`

---

## Connect to PostgreSQL Server

The PostgreSQL server is **automatically registered** in pgAdmin at startup using `pgadmin/servers.json`.
And schema generated using `sql/init.sql`

When you open pgAdmin, the server will already be available under:

```text
Servers
└── etl-postgres
---

## Navigate Tables

In pgAdmin, go to:

```
Servers > ETL Server > Databases > etl_db > Schemas > bronze | silver | gold > Tables
```

---

## Shut Down

To stop all containers:

```bash
docker compose down
```

---

## Default Credentials Summary

| Component     | URL                    | Username           | Password     |
|---------------|-------------------------|---------------------|--------------|
| JupyterLab    | http://localhost:8888   | (use token from logs) | —          |
| pgAdmin       | http://localhost:5050   | admin@example.com     | admin        |
| PostgreSQL    | —                       | postgres            | postgres     |
