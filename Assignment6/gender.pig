users = LOAD '/Users/Sattya/Documents/Sattya_MS/Big_Data/HW6/users.dat' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+)::([^\\:]+)::([^\\:]+)::([^\\:]+)::([^\\:]+)') as (userId, gender, age, occupation, zip);

grpd = group users by gender;
cnt = foreach grpd { gender = users.gender; generate group, COUNT(gender);};
dump cnt;