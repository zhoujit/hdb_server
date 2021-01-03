create table Issuers
(
    Id varchar pk lz4,
    Name varchar lz4,
    Price double lz4,
);

create table Issuers2
(
    Id varchar pk lz4,
    Name varchar lz4,
    Price double lz4,
    PriceDate date lz4,
    LastUpdate datetime lz4,
);


show tables;

insert into Issuers(Id, Name, Price) values('S2020', 'ARM 2020', 2020.12345),('S2021', 'ARM 2021', 2021.12345);

insert into Issuers(Id, Name, Price) values('S011', 'ARM 001', 1),('S012', 'ARM 002', 2);
insert into Issuers(Id, Name, Price) values('S013', 'ARM 001', 100),('S014', 'ARM 002', 20);
insert into Issuers(Id, Name, Price) values('S015', 'ARM 001', 10000),('S016', 'ARM 0022', 2000);

insert into Issuers(Id, Name, Price) values('S021', 'ARM S002', 1);
insert into Issuers(Id, Name, Price) values('S022', 'ARM S022', 100);
insert into Issuers(Id, Name, Price) values('S023', 'ARM S023', 10000);
insert into Issuers(Id, Name, Price) values('S024', 'ARM S024', 1);
insert into Issuers(Id, Name, Price) values('S025', 'ARM S025', 100);
insert into Issuers(Id, Name, Price) values('S026', 'ARM S026', 10000);


select * from Issuers where Id='S001' or Id='S002';
select * from Issuers where Id='B10087' or Id='B19987';


select * from Issuers where Id>'B19987' or Id <='B10002';
select * from Issuers where Id>='B19997' or Id='B19966' or Id like 'B1055%';
select * from Issuers where Name like 'BATCH_10088%';
select * from Issuers where Price >= 280;

select top 5 * from Issuers where Name like 'BATCH_10088%';
select * from Issuers where Price >= 280 limit 5;

-- Currently in and operator, don't allow same column name, for example: Id>'B19901' and Id<'B19909'.
select * from Issuers where Id>'B19909' and Name <='BATCH_10088';
select * from Issuers where Id>'B19909' and Name like 'BATCH_10088%';


delete from Issuers where Id='S2020';


truncate table Issuers;


drop table Issuers;


stop;



set output csv;
set output tabfile;
set output textfile;
set output console;

set output compress;
set output uncompress;


imp Issuers file=d:\temp\imptest.txt;

server imp Issuers file=./log/imptest_100K.txt logfile=./log/imptest_100K.log;

-- exp Issuers file=d:\temp\exptest.txt


