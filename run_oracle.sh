docker run -it --name tester \
-p 1521:1521 \
-e ORACLE_PWD=tiger \
-e ENABLE_ARCHIVELOG=false \
-e ENABLE_FORCE_LOGGING=false \
container-registry.oracle.com/database/free:latest-lite