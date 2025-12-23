# IFCB REST API

This repository contains an initial, experimental implementation of a REST API for interacting iwth IFCB data and metadata.

As of Dcecember 2025 it is not ready to be used in production.

It is based on the [AMPLIfy stateless microservice framework](https://github.com/WHOIGit/amplify-stateless-microservice) and the [AMPLIfy storage utilities framework](https://github.com/WHOIGit/amplify-storage-utils). See those repositories for details about the frameworks and utilities.

## Installation and deployment

The recommended way to deploy this service is via Docker (see `docker-compose.ymk`), but it can also be run outside of Docker by following these steps in your clone:

### Configure the backend

The environment variable `IFCB_RAW_DATA_DIR` must be set to the pathname of a directory containing raw IFCB data.

### Install and run the service

```
pip install '.[deploy]'
uvicorn raw-data-service.main:app --host 0.0.0.0 --port 8001
```

If you want to develop, it is convenient to use the `-e` option on pip and the `--reload` option on `uvicorn`, like this:

```
pip install -e '.[deploy]'
uvicorn raw-data-service.main:app --host 0.0.0.0 --port 8001 --reload
```

Once running, the API is accessible using endpoints documented in Swagger/OpenAPI which you can access at `http://localhost:8001/docs`.