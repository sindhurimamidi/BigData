data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

grpd = GROUP data by locationDesc;
crimesByLocationDesc = FOREACH grpd { location = data.locationDesc; GENERATE group, COUNT(location) as count;};

STORE crimesByLocationDesc INTO 'hdfs:/user/Sattya/pigoutputs/crimesByLocationDesc' ;

ascRating = order crimesByLocationDesc by count asc;
top10safeLocations = limit ascRating 11;
descRating = order crimesByLocationDesc by count desc;
top10dangerousLocations = limit descRating 10;

--STORE top10safeLocations  INTO 'hdfs:/user/Sattya/pigoutputs/crimesByLocationDesc_safe10' ;
STORE top10dangerousLocations  INTO 'hdfs:/user/Sattya/pigoutputs/crimesByLocationDesc_dangerous10' ;


