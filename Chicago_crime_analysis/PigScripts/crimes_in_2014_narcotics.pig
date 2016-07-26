REGISTER myudfs.jar;

data = LOAD 'hdfs:/user/Sattya/datasets/crimes.csv' USING PigStorage(',') AS (id:chararray, caseNumber:chararray, date:chararray, block:chararray, iucr:chararray, primaryType:chararray, description:chararray, locationDesc:chararray, arrest:chararray, domestic:chararray, beat:chararray, district:chararray, ward:chararray, community:chararray, fbiCode:chararray, xCoordinate:chararray, yCoordinate:chararray, year:chararray, lastUpdate:chararray, latitude:chararray, longitude:chararray);

data_2014 = filter data by year == '2014';
data_2014_narcotics = filter data_2014 by primaryType == 'CRIMINAL DAMAGE';
data_2014_narcotics_month = FOREACH data_2014_narcotics GENERATE myudfs.DateToMonth(date) as month, id;

grpd = GROUP data_2014_narcotics_month by month;
crimesByMonth = FOREACH grpd { month = data_2014_narcotics_month .month; GENERATE group, COUNT(month) as count;};

STORE crimesByMonth INTO 'hdfs:/user/Sattya/pigoutputs/crimesCriminalDamage2014' ;

