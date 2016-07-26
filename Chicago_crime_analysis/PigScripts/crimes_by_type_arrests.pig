--REGISTER myudfs.jar;

data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

data_arrests = filter data by arrest == 'TRUE';

grpd = GROUP data_arrests by primaryType;
crimesByTypeArrests = FOREACH grpd { type = data_arrests.primaryType; GENERATE group, COUNT(type) as count;};

descRating = order crimesByTypeArrests by count desc;
crimesByTypeArrests_top15 = limit descRating 15;

STORE crimesByTypeArrests  INTO 'hdfs:/user/Sattya/pigoutputs/crimesByTypeArrests' ;
STORE crimesByTypeArrests_top15 INTO 'hdfs:/user/Sattya/pigoutputs/crimesByTypeArrestsTop15' ;
