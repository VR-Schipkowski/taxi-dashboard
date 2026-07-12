# BD26_Project_W4_B

# Run local

from the main folder you can run 
```bash
docker compose up -d --build
```
If you need to rebuild the project you can run 
```bash
docker compose down -v
```

you will find the web interface under 
```
http://localhost:5173/
```
make sure that _frontend/src/config.js_ has correct BACKEND and WS_LINK variables
# Cloud deployment

current cloud_deploy branch which is regularly updated to represent **main** is deployed on GCP under
```
http://34.28.224.202:5173/
```
# Link to docker images on docker hub
You can find all of our docker images in docker hub under:

```
https://hub.docker.com/repository/docker/gesadolkemeyer/bd26_project_w4_b/tags
```
You will need the `docker-compose.yaml` of this repo to use the images.

