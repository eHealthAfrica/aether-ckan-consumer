set -e
cd ckan
{ # try
    docker-compose -f docker-compose.yml build
} || { # catch
    echo 'not ready...'
}

docker-compose -f docker-compose.yml up -d
until docker exec -it ckan /usr/local/bin/ckan-paster --plugin=ckan sysadmin -c /etc/ckan/production.ini add admin
do
    echo "waiting for ckan container"
    sleep 5
done

