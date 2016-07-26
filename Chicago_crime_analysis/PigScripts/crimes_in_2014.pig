--REGISTER myudfs.jar;

data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

data_2014 = filter data by year == '2014';

grpd = GROUP data_2014 by primaryType;
crimesIn2014 = FOREACH grpd { type = data_2014.primaryType; GENERATE group, COUNT(type) as count;};

descRating = order crimesIn2014 by count desc;
crimesIn2014_top15 = limit descRating 15;

STORE crimesIn2014 INTO 'hdfs:/user/Sattya/pigoutputs/crimesIn2014' ;
STORE crimesIn2014_top15 INTO 'hdfs:/user/Sattya/pigoutputs/crimesIn2014top15' ;
