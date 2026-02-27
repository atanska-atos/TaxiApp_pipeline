-- setup

CREATE CATALOG IF NOT EXISTS transportation_app;

CREATE SCHEMA IF NOT EXISTS transportation_app.bronze;
CREATE SCHEMA IF NOT EXISTS transportation_app.silver;
CREATE SCHEMA IF NOT EXISTS transportation_app.gold;