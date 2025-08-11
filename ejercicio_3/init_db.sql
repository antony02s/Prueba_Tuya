-- Crea usuario y base para trabajar
CREATE ROLE app_user WITH LOGIN PASSWORD 'app_pass';
CREATE DATABASE app_db OWNER app_user;
GRANT ALL PRIVILEGES ON DATABASE app_db TO app_user;