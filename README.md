

# README.md

## Lance Data Viewer (v0.1) — read-only web UI for Lance datasets

Browse Lance tables from your local machine in a simple web UI. No database to set up. Mount a folder and go.

### Quick start (Docker)

1. **Pull**

```bash
docker pull ghcr.io/gordonmurray/lance-data-viewer:latest
```

2. **Run (mount your data)**

```bash
docker run --rm -p 8080:8080 \
    -v /path/to/your/lance:/data:ro \
    ghcr.io/gordonmurray/lance-data-viewer:latest
```

3. **Open the UI**

```
http://localhost:8080
```

### What counts as “Lance data” here?

A folder containing Lance tables (as created by Lance/LanceDB). The app lists tables under `/data`.

### Features (v0.1)

- Read-only browsing with organized left sidebar (Datasets → Columns → Schema).
- Schema view with vector column highlighting.
- Server-side pagination with inline controls.
- Column selection and filtering.
- Responsive layout optimized for data viewing.

### Health check

```
GET http://localhost:8080/healthz
```

### Configuration (optional)

- **Port:** change host port with `-p 9000:8080`.
- **Read-only mount:** keep `:ro` to avoid accidental writes in future versions.

### Images & registries

- **GitHub Container Registry** (`ghcr.io/gordonmurray/lance-data-viewer:TAG`).

### Build and test locally

```bash
# Build the Docker image
docker build -f docker/Dockerfile -t lance-data-viewer:dev .

# Make your Lance data readable (one-time setup)
chmod -R o+rx data

# Run with your data (replace 'data' with your lance folder path)
docker run --rm -p 8080:8080 -v $(pwd)/data:/data:ro lance-data-viewer:dev

# Open the web interface
open http://localhost:8080

# Test the API endpoints
curl http://localhost:8080/healthz
curl http://localhost:8080/datasets
curl "http://localhost:8080/datasets/your-dataset/rows?limit=5"
```

### Development workflow

```bash
# Stop any running containers
docker ps -q | xargs docker stop

# Rebuild after code changes
docker build -f docker/Dockerfile -t lance-data-viewer:dev .

# Run in background
docker run --rm -d -p 8080:8080 -v $(pwd)/data:/data:ro lance-data-viewer:dev

# View logs
docker logs $(docker ps -q --filter ancestor=lance-data-viewer:dev)
```

### Security notes

- Container runs as non-root.
- No authentication in v0.1; bind to localhost during development and run behind a reverse proxy if exposing.