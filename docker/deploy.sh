#! /bin/bash

if [ -z "$SERVER_IP_ADDRESS" ]
then
    echo "SERVER_IP_ADDRESS not defined"
    exit 0
fi

git archive --format tar --output ./project.tar master

echo "Uploading the project.....:-)...Be Patient!"
rsync ./project.tar root@$SERVER_IP_ADDRESS:/tmp/project.tar
echo "Upload complete....:-)"

echo "Uploading the .env file..."
scp ./.env root@$SERVER_IP_ADDRESS:/app/.env
echo ".env upload complete....:-)"

echo "Building the image......."
ssh -o StrictHostKeyChecking=no root@$SERVER_IP_ADDRESS << 'ENDSSH'
    mkdir -p /app
    rm -rf /app/* && tar -xf /tmp/project.tar -C /app
    docker compose -f /app/docker/docker-compose.yml build
ENDSSH
echo "Build completed successfully.......:-)"