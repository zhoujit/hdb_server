create table Issuers
(
    Id varchar pk lz4,
    Name varchar lz4,
    Price double lz4,
);


show tables;


insert into Issuers(Id, Name, Price) values('S2020', 'ARM 2020', 2020.12345),('S2021', 'ARM 2021', 2021.12345);


select * from Issuers where Id = 'S2020' or Id = 'S2021';


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


-- exp Issuers file=d:\temp\exptest.txt

