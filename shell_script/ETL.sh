#!/bin/zsh


# Copy postgres scripts into folder before running

#Set working dir
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

# download data file
curl -LO http://www.travstat.se/travdata2002-2020.zip

# unzip data file 
unzip -o travdata2002-2020.zip
mkdir ./racedata
unzip -o  travdata2002.zip -d ./racedata

# delete unneeded files
find ./racedata -type f -not \( -name  "lopp.txt" -o -name "tvl.txt" -o -name "prog.txt" -o -name "klass.txt" -o -name "variab.txt" \) | xargs rm

# check if textfiles are utf-8, if not: use iconv to convert (for loop over .txt files in dir)
for f in $DIR/racedata/*
do
        iconv -f ISO-8859-1 -t UTF-8 "$f" >  "${f%.txt}.utf" 
done

rm $DIR/racedata/*.txt

for i in $DIR/racedata/*
do
    sed 's/\\N/NULL/g' "$i" > "${i%.utf}.txt"
done

rm $DIR/racedata/*.utf

echo done

# create tables in postgres
psql -U postgres -d postgres -a -f ETL.sql 

# load datafiles into postgres tables
#psql postgres postgres -c '\copy lopp FROM './racedata/lopp.txt' 'TXT''

# run postgres script to create flat table and clean variables
#psql -U postgres -d postgres -a -f create_flat_table.sql 

#copy flat table to AWS database
