data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id, caseNumber, date, block, iucr, primaryType, description, locationDesc, arrest, domestic, beat, district, ward, community, fbiCode, xCoordinate, yCoordinate, year, lastUpdate, latitude, longitude);

grpd = GROUP data by block;
--crimesByBlock = FOREACH grpd { block = data.block; GENERATE group, COUNT(block) as count;};
crimesByBlock = FOREACH grpd  GENERATE group, COUNT(data) as count;
STORE crimesByBlock INTO 'hdfs:/user/Sattya/pigoutputs/crimesByBlock_dup' USING org.apache.pig.piggybank.storage.CSVExcelStorage() ;

ascRating = order crimesByBlock by count asc;
lowest20blocksByCrime = limit ascRating 20;
descRating = order crimesByBlock by count desc;
top20blocksByCrime = limit descRating 20;

STORE lowest20blocksByCrime INTO 'hdfs:/user/Sattya/pigoutputs/crimesByBlock_lowest20_dup' USING org.apache.pig.piggybank.storage.CSVExcelStorage() ;
STORE top20blocksByCrime  INTO 'hdfs:/user/Sattya/pigoutputs/crimes/crimesByBlock_top20_dup' USING org.apache.pig.piggybank.storage.CSVExcelStorage() ;
