REGISTER myudfs.jar;

data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

type_location = FOREACH data GENERATE myudfs.TypeDescription(primaryType, locationDesc) as key, id;
grpd = GROUP type_location by key;
crimesByTypeLoc = FOREACH grpd { type_loc = type_location.key; GENERATE group, COUNT(type_loc) as count;};

STORE crimesByTypeLoc INTO 'hdfs:/user/Sattya/pigoutputs/crimesByTypeLocation' ;
