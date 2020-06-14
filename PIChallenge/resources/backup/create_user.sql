use testing_etl;
go
CREATE LOGIN pi WITH PASSWORD = 'AdminPI!';
go
create user pi for login pi;
go
grant control on database::Testing_ETL to pi;
go
