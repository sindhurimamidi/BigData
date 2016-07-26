data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id, caseNumber, date, block, iucr, primaryType, description, locationDesc, arrest, domestic, beat, district, ward, community, fbiCode, xCoordinate, yCoordinate, year, lastUpdate, latitude, longitude);

grpd = GROUP data by year;
crimesByYear = FOREACH grpd  GENERATE group, COUNT(data) as count;
STORE crimesByYear INTO 'hdfs:/user/Sattya/scratch/Pig' USING org.apache.pig.piggybank.storage.CSVExcelStorage() ;

