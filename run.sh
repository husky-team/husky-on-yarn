mvn package

if [ -z "$1" ]; then
    yarn jar target/husky-yarn-1.0-alpha.jar husky.client.HuskyYarnClient -help
    exit 0
fi

yarn jar target/husky-yarn-1.0-alpha.jar husky.client.HuskyYarnClient \
-app_name HuskyOnYarn \
-container_memory 8192 \
-master_memory 2048 \
-worker_infos localhost:2 \
-master $1 \
-application $2 \
-config $3

