# DataSHIELD Vault

Containerized vault for file storage and hashing for DataSHIELD.

## Features

- **MinIO Object Store**: S3-compatible storage backend
- **Vault API**: HTTP API for listing objects, downloading files, and fetching hashes
- **Filesystem Watcher**: Automatically discovers collections and uploads objects
- **Per-Collection API Keys**: Each collection gets its own API key (auto-generated)

## Quick Start

From the repository root:

```bash
./vault.sh
```

This will:
- Create `data/collections/` if needed
- Check port availability (auto-assigns new ports if needed)
- Build and start the containers

## Adding Collections

Simply create a folder in `data/collections/`:

```bash
mkdir data/collections/my-cohort
```

A `.vault_key` file will be automatically created with the API key for that collection.

Then add files:

```bash
cp myfile.nii.gz data/collections/my-cohort/
```

Get the API key:

```bash
cat data/collections/my-cohort/.vault_key
```

## Project Structure

```
datashield-vault/
├── vault.sh                    # Main entry point
├── app/                        # Application code
│   ├── src/vault/              # Python package
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── pyproject.toml
├── data/                       # Created by run.sh
│   └── collections/            # Drop folders/files here
└── LICENSE
```

## API Endpoints

All endpoints require the `X-Collection-Key` header with the collection's API key.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/collections/{collection}/objects` | List objects in collection |
| GET | `/api/v1/collections/{collection}/hashes` | List all object hashes |
| GET | `/api/v1/collections/{collection}/objects/{name}` | Download an object |
| GET | `/api/v1/collections/{collection}/hashes/{name}` | Get hash of single object |
| GET | `/health` | Health check (no auth required) |

## Usage Examples

```bash
# Get the API key
KEY=$(cat data/collections/my-cohort/.vault_key)

# List objects in a collection
curl -H "X-Collection-Key: $KEY" \
  http://localhost:8000/api/v1/collections/my-cohort/objects

# Get all hashes
curl -H "X-Collection-Key: $KEY" \
  http://localhost:8000/api/v1/collections/my-cohort/hashes

# Download an object
curl -H "X-Collection-Key: $KEY" \
  -o file.nii.gz \
  http://localhost:8000/api/v1/collections/my-cohort/objects/file.nii.gz

# Get single object hash
curl -H "X-Collection-Key: $KEY" \
  http://localhost:8000/api/v1/collections/my-cohort/hashes/file.nii.gz
```

## Port Configuration

Default port: 8000

If the port is in use, `vault.sh` will automatically find the next available port.

Override via argument:

```bash
./vault.sh 8080    # API on port 8080
```

## Stopping

```bash
./vault.sh stop
```

## License

MIT
