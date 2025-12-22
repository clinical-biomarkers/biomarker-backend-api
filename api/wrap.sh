#srv="prd"
srv="dev"


sudo systemctl stop docker-biomarker-api-$srv.service
sudo python3 create_api_container.py $srv
sudo systemctl start docker-biomarker-api-$srv.service



