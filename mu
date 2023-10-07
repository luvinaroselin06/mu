To get Daemons
cd hadoop-2.9.1
bin/hadoop namenode -format
sbin/start -all.sh
jps
Datanode(wont get)
Go to files
Click Hadoop(dir)---&gt;delete datanode
Come terminal
sbin/hadoop-daemon.sh start datanode
jps
open new terminal after startng all daemons
cd
cd apache-hive-2.3.5-bin
bin/hive
1] GROUPBY, SORTBY, ORDERBY, CLUSTERBY, DISRTIBUTIVEBY OPERATIONS
ON MOVIE DATASET USING HIVE:
 create table user_1(userid string,uname string,uemaild string,uphno string) row format delimited fields terminated
by&#39;,&#39;stored as textfile;
 create table movie_1(mid string,mname string,category string) row format delimited fields terminated by &#39;,&#39; stored as
textfile;
 create table review_1(movieid string,userid string,rating string) row format delimited fields terminated by &#39;,&#39; stored
as textfile;
 LOAD DATA LOCAL INPATH&#39;/home/grg/user_11.txt&#39; into table user_1;
 LOAD DATA LOCAL INPATH&#39;/home/grg/review_11.txt&#39; into table review_1;
 LOAD DATA LOCAL INPATH&#39;/home/grg/movie_11.txt&#39; into table movie_1;
 select userid,movieid,rating from review_1 order by rating DESC;
 select rating,count(*) from review_1 group by rating;
 select mid,mname from movie_1 sort by mname;
 select mid,mname from movie_1 cluster by mid;
 select mid,mname from movie_1 distribute by mid;
user_11.txt
1,arun,arun@gmail.com,9445829999
2,ajay,ajay@gmail.com,8976542345
3,arvind,arvind@gmail.com,987654320
4,arav,arav@gmail.com,9870987098
5,ragu,ragu@gmail.com,987654321
movie_11.txt
01,dev,romantic
02,kgf,action
03,comali,comedy
04,lisa,horror
05,coco,comedy
06,june,comedy
review_11.txt
m1,u1,5

m2,u3,3
m3,u4,3
m4,u5,4.5
m5,u6,3
m6,u7,3

2]JOIN OPERATIONS USING HIVE
 create table customer(cus_id int,name string,acc_type string,address string,acc_bal float)row format delimited fields
terminated by &#39;,&#39; stored as textfile;
 create table transaction(tr_id int,cus_id int,tr_type string,amt float,year date)row format delimited fields
terminated by &#39;,&#39; stored as textfile;
 load data local inpath &#39;/home/grg/customer1.txt&#39; into table customer;
 load data local inpath &#39;/home/grg/transaction1.txt&#39; into table transaction;
 select A.name, A.acc_type, A.address, A.acc_bal, B.tr_id, B.tr_type, B.amt, B.date from customer A join transaction B
on (A.cus_id = B.cus_id);
 select A.name, A.acc_type, A.address, A.acc_bal, B.tr_id, B.tr_type, B.amt from customer A right outer join
transaction B on (A.cus_id = B.cus_id);
 select A.name, A.acc_type, A.address, A.acc_bal, B.tr_id, B.tr_type, B.amt from customer A full outer join transaction
B on (A.cus_id = B.cus_id);
 select A.name, A.acc_type, A.address, A.acc_bal, B.tr_id, B.tr_type, B.amt from customer A left outer join transaction
B on (A.cus_id = B.cus_id);
Customer1.txt
1,aadhi,current,peelamedu,35000.0
2 kaviya,saving,Rkpuram,20000.0
3,janani,fixed,ganapathy,21000.0
4,anu,fixed,peelamedu,35000.0
5,harini,saving,townhall,29000.0
Transaction1.txt
10,1,cash,25000,2021
11,2,cash,29000,2020
12,4,cash,5000,2021
13,3,cash,19000,2020
14,5,cash,19000,2020

3]VIEWS, INDEXES AND PARTITIONS USING HIVE
 create table customer027(userid string, name string, status string, country string,amount string) row format delimited
fields terminated by &#39;,&#39; stored as textfile;
 load data local inpath &#39;/home/grg/customer003.txt&#39; into table customer027;
 select * from customer027;
 create view customerview028 as select * from customer027 where amount&gt;20000;
 select * from customerview028;
 CREATE INDEX index10 ON TABLE customer027(userid) as &#39;org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler&#39;
WITH DEFERRED REBUILD;
 show index on customer027;
 create table customerpart025 (userid string, name string, status string) partitioned by (country string) row format
delimited fields terminated by &#39;,&#39;;

 show partitions customerpart025;
 INSERT OVERWRITE TABLE customerpart025 PARTITION(country) SELECT
userid,name,status,country from customer027;
Here you will get a error
 create view partitionview023 as select * from customer027 where country=&#39;us&#39;;
 select * from partitionview023;
 Describe customerpart025;
INPUT FILE:
1,rahul,yes,us,50000
2,aswin,no,india,25000
3,siva,yes,us,45000
4,agnel,no,india,17000
5,madhu,yes,saudi,20000
4]MANIPULATION OF CRICKET DATASET USING HIVE
 create table cricket(id string, name string, year string, runs float) row format delimited fields terminated by &#39;,&#39;
stored as textfile;
 load data local inpath &#39;/home/grg/cricket.txt&#39; into table cricket;
 SELECT year, max(runs) FROM cricket GROUP BY year;
 SELECT a.year, a.id, a.runs from cricket a JOIN (SELECT year, max(runs) runs FROM cricket GROUP BY year ) b ON (a.year
= b.year AND a.runs = b.runs);
cricket.txt
1,rahul,2016,90
2,raina,2018,98
3,virat,2019,99
4,bravo,2017,93
5,rayudu,2018,89
6,dhoni,2017,90
7,sachin,2018,48
8,rohit,2020,94
9,varun,2018,70
10,yadav,2020,87
5] EXTRACTION OF JSON DATA USING HIVE QUERY
 create table employee(str string);
 load data local inpath &#39;/home/grg/emp.json&#39; overwrite into table employee;
 select get_json_object(str,&#39;$.id&#39;)as eid,get_json_object(str,&#39;$.name&#39;)as
ename,get_json_object(str,&#39;$.designation&#39;)as edesignation,get_json_object(str,&#39;$.salary&#39;)as
 esalary,get_json_object(str,&#39;$.city&#39;)as city from employee;
Emp.txt
{ &quot;id&quot;:&quot;101&quot;,&quot;name&quot;:&quot;Aarya&quot; ,&quot;designation&quot;:&quot;sales&quot; ,&quot;salary&quot;:&quot;22000&quot;,&quot;city&quot;:&quot;Karur&quot;}
{ &quot;id&quot;:&quot;102&quot;,&quot;name&quot;:&quot;Bavani&quot;,&quot;designation&quot;:&quot;Analyst&quot;,&quot;salary&quot;:&quot;40000&quot;,&quot;city&quot;:&quot;Salem&quot;}
{ &quot;id&quot;:&quot;103&quot;,&quot;name&quot;:&quot;Joseph&quot;,&quot;designation&quot;:&quot;Manager&quot;,&quot;salary&quot;:&quot;50000&quot;,&quot;city&quot;:&quot;Erode&quot;}
{ &quot;id&quot;:&quot;104&quot;,&quot;name&quot;:&quot;Sarany&quot;,&quot;designation&quot;:&quot;Enginer&quot;,&quot;salary&quot;:&quot;35000&quot;,&quot;city&quot;:&quot;Kovai&quot;}
{ &quot;id&quot;:&quot;105&quot;,&quot;name&quot;:&quot;Vino1&quot; ,&quot;designation&quot;:&quot;Clerk&quot; ,&quot;salary&quot;:&quot;20000&quot;,&quot;city&quot;:&quot;Erode&quot;}
6]ANALYSIS OF AIRPORT DATA USING HIVE

 create table Airport(a_id string,name string,country string,IATA_FAA string,ICAO string,latitude string,longitude
string,altitude string,time_zone string,DST string,TZ string) row format delimited fields terminated by &#39;,&#39; stored as
textfile;
 create table Airlines(airline string,name string,alias string,IATA string,ICAO string,callsign string,country
string,active string) row format delimited fields terminated by &#39;,&#39; stored as textfile;
 create table Route (airlines string,airline_id string,source_airport string,source_airport_id
string,destination_airport string,destination_airport_id string,code_share string,stops string,equipments string) row
format delimited fields terminated by &#39;,&#39; stored as textfile;
 load data local inpath &#39;/home/grg/airport.txt&#39; overwrite into table Airport;
 load data local inpath &#39;/home/grg/airline.txt&#39; overwrite into table Airlines;
 load data local inpath &#39;/home/grg/routes.txt&#39; overwrite into table Route;
 select * from Airport limit 3;
 select * from Route where stops=&#39;0&#39;;
Airlines.txt
10,spicejet,IA,BEP,BEPO,citrus,India,N
11,Deccan Airlines,EA,RST,RSTO,jazz,India,Y
12,Air India,JA,BLR,BLRO,redwood,India,N
13,Air Asia,AI,ALT,ALTO,dragon,America,N
14,Indigo,BA,ELT,ELTO,cactus,Europe,N
Airport.txt
11,madurai Airport,Salem,India,BEP,BEPO,56.2,25.1,26,5.5,I,india.zone
12,coimbatore Airport,coimbatore,India,RST,RSTO,67.3,27.1,67,6.7,C,india.zone
13,trichy Airport,Madurai,India,BLR,BLRO,54.3,32.7,78,4.5,M,india.zone
14,alaska Airport,alaska,America,ALT,ALTO,67.2,57.4,67,7.2,N,america.zone
15,bruges,flemish,Europe,ELT,ELTO,92.2,67.2,89,6.1,L,europe.zone
Routes.txt
BEP,21,BEPO, ALTO,BFGF,415,N,0,TUV
RST,22,RSTO,THET,RETO,456,N,1,VUT
BLR,23,BLRO,FACO,DOCT,430,Y,0,YUT
ALT,24,ALTO,GORT,TORY,311,N,0,TUR
BLT,25,BLTO,HORT,ROTY,378,N,3,GUT
