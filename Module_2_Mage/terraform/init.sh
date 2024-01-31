#!/bin/bash
account=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/account)

cd /home/$account

# prepare 
mkdir -p ./data/pgadmin
chown -R 5050:5050 ./data/pgadmin

# Install docker and docker compose
curl -fsSL https://get.docker.com -o install-docker.sh
sudo sh install-docker.sh

# # Download docker-compose file for mage and pg_admin
wget https://raw.githubusercontent.com/alfredzou/Zoomcamp2024/main/Module_2_Mage/terraform/docker-compose.yaml

# # Allow the running of docker without sudo
groupadd docker
usermod -aG docker $account
newgrp docker
