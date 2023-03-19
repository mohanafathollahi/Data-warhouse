-----------Dimension1: AIRCRAFT_info----------
CREATE TABLE AIRCRAFT_info(
aircraft_id CHAR(6) NOT NULL,
model VARCHAR(8) NOT NULL,
manufacturer VARCHAR(10) NOT NULL,
PRIMARY KEY(aircraft_id));

INSERT INTO AIRCRAFT_info VALUES ('XY-ALV','A340','Airbus'); 
INSERT INTO AIRCRAFT_info VALUES ('XY-CMJ','737','Boeing'); 
INSERT INTO AIRCRAFT_info VALUES ('XY-FBA','A330neo','Airbus');
INSERT INTO AIRCRAFT_info VALUES ('XY-IOO','777','Boeing');
INSERT INTO AIRCRAFT_info VALUES ('XY-IWX','747','Boeing');
INSERT INTO AIRCRAFT_info VALUES ('XY-JXR','A350 XWB','Airbus');

-----------Dimension2: Time----------
CREATE TABLE time_dmy(
time_id DATE NOT NULL,
day_ Integer NOT NULL,
MONTH_ varCHAR(8) NOT NULL,
YEAR_ Integer NOT NULL,
PRIMARY KEY(time_id));


INSERT INTO time_dmy VALUES ('12/02/2021',1,'February', '2021');
INSERT INTO time_dmy VALUES ('02/02/2021',2,'February', '2021');
INSERT INTO time_dmy VALUES ('06/02/2020',3,'February', '2021');

INSERT INTO time_dmy VALUES ('02/02/2020',12,'February', '2020');
INSERT INTO time_dmy VALUES ('03/02/2020',6,'February', '2020');
INSERT INTO time_dmy VALUES ('04/02/2020',11,'February', '2020');
INSERT INTO time_dmy VALUES ('05/02/2020',28,'February', '2020');
INSERT INTO time_dmy VALUES ('23/12/2020',12,'December', '2020');
INSERT INTO time_dmy VALUES ('24/12/2020',10,'December', '2020');
INSERT INTO TIME_DMY VALUES ('02/05/2015',10,'May', '2015');
INSERT INTO TIME_DMY VALUES ('13/02/2021',11,'February', '2021');

-------------------------------Fact1: AIRCRAFT_METRICS-------------------------------
CREATE TABLE AIRCRAFT_METRICS(
aircraft_id CHAR(6) NOT NULL,
time_id DATE NOT NULL,
airport CHAR(3) NOT NULL,
FH FLOAT NOT NULL,
TO_ Integer NOT NULL,
ADOSS Integer NOT NULL,
ADOSU Integer NOT NULL,
DY FLOAT NOT NULL,
CN Integer NOT NULL,
PRIMARY KEY(aircraft_id, time_id),
fOREIGN KEY(aircraft_id) REFERENCES AIRCRAFT_info(aircraft_id),
fOREIGN KEY(time_id) REFERENCES time_dmy(time_id));


INSERT INTO AIRCRAFT_METRICS VALUES ('XY-CMJ','04/02/2020','BEJ',4,1,0,0,16,0);
INSERT INTO AIRCRAFT_METRICS VALUES ('XY-JXR','12/02/2021', 'BEJ',0,0,1,0,0,1);
INSERT INTO AIRCRAFT_METRICS VALUES ('XY-JXR','13/02/2021', 'BEJ',5,1,0,0,40,0);

INSERT INTO AIRCRAFT_METRICS VALUES ('XY-IWX','03/02/2020','AAR',5,2,0,0,0,0);
INSERT INTO AIRCRAFT_METRICS VALUES ('XY-IWX','04/02/2020','AAR',89,5,0,0,50,0);
INSERT INTO AIRCRAFT_METRICS VALUES ('XY-IWX','05/02/2020','AbR',0,0,1,0,0,1);
INSERT INTO AIRCRAFT_METRICS VALUES ('XY-IWX','06/02/2020','AAR',10,100,0,0,0,0);


-------------------------------Fact2: LOOGBOOKS-------------------------
CREATE TABLE LOGBOOKS (
aircraft_id CHAR(6) NOT NULL,
time_id DATE NOT NULL,
reporteur_id INTEGER  NOT NULL,
airport CHAR(5) NOT NULL,
plogbook INTEGER NOT NULL,
mlogbook INTEGER NOT NULL,

primary KEY(aircraft_id,time_id,reporteur_id),
fOREIGN KEY(aircraft_id) REFERENCES AIRCRAFT_info(aircraft_id),
fOREIGN KEY(time_id) REFERENCES time_dmy(time_id));

INSERT INTO LOGBOOKS  VALUES ('XY-ALV','02/02/2020', 1, 'AAR', 1, 0);
INSERT INTO LOGBOOKS  VALUES ('XY-CMJ','04/02/2020', 2, 'AAR', 1, 0);
INSERT INTO LOGBOOKS VALUES ('XY-CMJ','24/12/2020',3,'AAR',3,2);
INSERT INTO LOGBOOKS VALUES ('XY-IWX','03/02/2020',3,'AAR',1,2);
INSERT INTO LOGBOOKS VALUES ('XY-JXR','12/02/2021',4,'BEJ',2,2);

*****************************QUERIES*****************************
------------------Part a------------------
       /*AS an Example TO answer part a*/
   /* without considering MATERIALIZED VIEW*/
SELECT fm.AIRCRAFT_ID, t.day_ , fm.FH AS T_FH, fm.TO_ AS T_TO
FROM AIRCRAFT_METRICS fm, AIRCRAFT_info ai, TIME_dmy t 
WHERE fm.AIRCRAFT_ID = ai.AIRCRAFT_ID AND t.time_ID = fm.time_id; 

-------------MATERIALIZED VIEW LOGS FOR EVERY TABLE INVOLVED IN THE VIEWS-------------

CREATE MATERIALIZED VIEW LOG ON AIRCRAFT_METRICS
WITH ROWID,SEQUENCE(AIRCRAFT_ID, time_ID,FH,TO_, ADOSS , ADOSU, DY,CN)
INCLUDING NEW VALUES;


CREATE MATERIALIZED VIEW LOG ON TIME_Dmy
WITH ROWID,SEQUENCE(time_id, MONTH_, YEAR_)
INCLUDING NEW VALUES;


CREATE MATERIALIZED VIEW LOG ON AIRCRAFT_info
WITH ROWID,SEQUENCE(AIRCRAFT_ID, MODEL, MANUFACTURER)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON LOGBOOKS
WITH ROWID,SEQUENCE(AIRCRAFT_ID,  time_ID, reporteur_id, airport,plogbook,mlogbook ) INCLUDING NEW VALUES;


-------------OPERATION MONTHLY FOR PART a & b-------------
CREATE MATERIALIZED VIEW OPERATIONS_MONTHLY
BUILD IMMEDIATE
REFRESH FAST
ON DEMAND
START WITH SYSDATE NEXT (ADD_MONTHS(TRUNC(SYSDATE,'MM'),1))
ENABLE QUERY REWRITE AS
SELECT fm.AIRCRAFT_ID, ai.MODEL, t.MONTH_, t.YEAR_, 
		SUM(fm.FH) AS T_FH, 
		SUM(fm.TO_) AS T_TO, 
		COUNT(fm.time_id) AS T_DAYS,
		SUM(fm.DY) AS S_DELAY,
	    COUNT(fm.DY) AS N_DELAY,
	    SUM(fm.cn) AS T_CN,
	    SUM(fm.ADOSS) AS T_ADOSS,
	    SUM(fm.ADOSU) AS T_ADOSU,
	    sum(fm.ADOSS+fm.ADOSU) AS ADOS  
	FROM AIRCRAFT_METRICS fm, TIME_DMY t, AIRCRAFT_info ai
	WHERE fm.time_id = t.time_id AND fm.AIRCRAFT_ID = ai.AIRCRAFT_ID 
	GROUP BY fm.AIRCRAFT_ID, ai.MODEL, t.MONTH_, t.YEAR_;

*****************************QUERIES*****************************
       /*AS an Example TO answer part b*/
    ------------------Part b------------------
SELECT aircraft_id, MONTH_, YEAR_,
	 (T_DAYS - ADOS) AS ADIS, ADOS, T_ADOSS, T_ADOSU,
	 (T_CN/T_TO)AS CNR,
	 (N_DELAY/T_TO) AS DYR,
	 100-((N_DELAY+T_CN)/T_TO)AS TDR,
	 (S_DELAY/N_DELAY) AS T_ADD  
FROM OPERATIONS_MONTHLY;

---------------------------------OPERATION MONTHLY FOR PART c & d----------------------------------

CREATE MATERIALIZED VIEW LOOGBOOK_MONTHLY
BUILD IMMEDIATE
REFRESH FAST
ON DEMAND
START WITH SYSDATE NEXT (ADD_MONTHS(TRUNC(SYSDATE,'MM'),1))
ENABLE QUERY REWRITE AS
	SELECT l.aircraft_id, l.airport, ai.MODEL, ai.MANUFACTURER, t.MONTH_, t.YEAR_,
	SUM (l.plogbook) AS T_PLOGBOOK, 
	SUM (l.mlogbook) AS T_MLOGBOOK,
	sum(l.plogbook+l.mlogbook) AS T_LOGBOOKs,
	sum(fm.FH) AS T_FH,
	sum(fm.TO_) AS T_TO
	FROM LOGBOOKS l, AIRCRAFT_METRICS fm, time_dmy t, AIRCRAFT_info ai
	WHERE l.aircraft_id = fm.aircraft_id AND l.aircraft_id = ai.aircraft_id AND 
	l.time_id = t.time_id  
	GROUP BY l.aircraft_id, l.airport, ai.MODEL, ai.MANUFACTURER, t.MONTH_,t.YEAR_ ;


*****************************QUERIES*****************************
       /*AS an Example TO answer part c*/
   ------------------part c------------------
SELECT aircraft_id, model, MANUFACTURER, MONTH_, YEAR_, 

	(1000*(T_LOGBOOKS/T_FH)) AS RRh,
	(100*(T_PLOGBOOK/T_TO)) AS RRc,
	(1000*(T_LOGBOOKS/T_FH)) AS PRRh,
	(100*(T_PLOGBOOK/T_TO)) AS PRRc
	
FROM LOOGBOOK_MONTHLY;

       /*AS an Example TO answer part d*/
   ------------------part d------------------
SELECT aircraft_id, airport, model, 

	((T_MLOGBOOK/T_FH)*1000) AS MRRh,
	((T_MLOGBOOK/T_TO)*100)AS MRRc
	
FROM LOOGBOOK_MONTHLY;

