create database Datawarehouse

if Exists (select 1 from sys.databases where name='Datawarehouse')
begin 
/*this line force the database to disconnet all connectors to only one as if the
database is connected to an app or and an other deverloper*/

	alter database datawarehouse set SINGE_USER with rollback immediate;
	drop database Datawarehouse
end
go
use Datawarehouse
create schema bronze
go
create schema silver
go
create schema gold
