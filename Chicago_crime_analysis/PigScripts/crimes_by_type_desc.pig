REGISTER myudfs.jar;

data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

type_desc = FOREACH data GENERATE myudfs.TypeDescription(primaryType, description) as key, id;
grpd = GROUP type_desc by key;
crimesByTypeDesc = FOREACH grpd { type_desc = type_desc.key; GENERATE group, COUNT(type_desc) as count;};

STORE crimesByTypeDesc INTO 'hdfs:/user/Sattya/pigoutputs/crimesByTypeDesc' ;
