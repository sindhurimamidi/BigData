--REGISTER myudfs.jar;

data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

data_theft = filter data by primaryType == 'THEFT';

grpd = GROUP data_theft by locationDesc;
crimesByTheftLoc = FOREACH grpd { loc = data_theft.locationDesc; GENERATE group, COUNT(loc) as count;};

descRating = order crimesByTheftLoc by count desc;
crimesByTheftLoc_top15 = limit descRating 15;

--STORE crimesByTheftLoc INTO '/Users/Sattya/Documents/Sattya_MS/Big_Data/Project/PigScripts/crimesByTheftLocation' ;
STORE crimesByTheftLoc_top15 INTO  'hdfs:/user/Sattya/pigoutputs/crimesByTheftLocationTop15';
