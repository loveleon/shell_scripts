#!/bin/bash
# By brian@brianyoungblood.com to keep original files and import based on tab delimited data
# orginal script based on work from Eric London. http://ericlondon.com/bash-shell-script-import-large-number-csv-files-mysql
# show commands being executed, per debug
#set -x

#details
#after cancle the first line(code,open,close,high,low....),we need to write the data to new files.
#1.make sure the new data file name.
#2.while+getline+write data
#3.new file: date + markt_code
#code,date,open,high,low,close,change1,volume,money,traded_market_value,market_value,turnover,adjust_price,report_type,report_date,PE_TTM,PS_TTM,PC_TTM,PB
#4.DAY_K_LINE_1_1990 .

# define database connectivity
#_db="mydatabase"
_db="kmdshistory"
_db_user="root"
_db_password="rootroot"
#_db_password="root"
#_db_host="172.16.30.184"
_db_host="localhost"
#_db_host="127.0.0.1"
_db_port="3306"

# define directory containing CSV files
_csv_directory="."

# go into directory
cd $_csv_directory

# get a list of CSV files in directory
_csv_files_origin=`ls -A *.csv`
#_csv_files_origin=`ls -l *.csv`
#_csv_files_origin=`ls -l *.txt`
#_csv_files_origin=`ls -1 *.txt`


# loop through files and fix the linefeeds
for _csv_file_origin in ${_csv_files_origin[@]}
do
_csv_file_origin_extensionless=`echo $_csv_file_origin | sed 's/\(.*\)\..*/\1/'`
# change linefeeds to unix sed for no dependancies
#echo "Changing linefeeds and removing date from column headers. This can take a little while. Saving as $_csv_file_origin_extensionless.fixed"
# sed -e 's/.$//' $_csv_file_origin > $_csv_file_origin_extensionless.fixed
done

# get a list of files in directory after we've fixed them
_csv_files=`ls -A *.csv`
#_csv_files=`ls -A *.txt`
#_csv_files=`ls -1 *.txt`

# loop through csv files
for _csv_file in ${_csv_files[@]}
do
# remove file extension
_csv_file_extensionless=`echo $_csv_file | sed 's/\(.*\)\..*/\1/'`
# define table name
_table_name="${_csv_file_extensionless}"
# get header columns from CSV file and remove the appending date from the name
_header_columns=`head -1 $_csv_directory/$_csv_file | tr ',' '\n' | sed -e 's/^"//' -e 's/"$//' -e 's/ /_/g'`
_header_columns=`echo $_header_columns | sed 's/change/change1/'`
_header_columns_string=`head -1 $_csv_directory/$_csv_file | tr '\t' ','`
_header_columns_string=`echo $_header_columns_string | sed 's/change/change1/'`
#_header_columns_string=`echo $_header_columns_string | sed 's/change/change1/'|sed 's/date/date1/'`
echo ""
echo "line67_header_columns_string:"
echo $_header_columns_string

#canle the head line of csv source file.eg,code,time,open.....
_code="code"
#echo ${_header_columns[@]}
for _header_in_code in ${_header_columns[@]}
do
    #echo $_header_in_code
    if [ $_code = $_header_in_code ]
    then
        sed -i '1d' $_csv_file
        echo ""
    fi
done

#after cancle the head line,then will deal with data line.
#sed  file 
function make_data_for_file(){
#FILENAME="$1"
FILENAME="$_csv_file"
_sh="sh"
_sz="sz"
_mark_tp=""
_table_name_prefix="DAY_K_LINE_"
#new table name like:DAY_K_LINE_1_1990
for i in  `cat $FILENAME`
do
    #    echo $i
    _data_columns=`echo $i |sed -e 's/^"//' -e 's/"$//' -e 's/ /_/g'`
    #_data_columns=`echo $i |tr ',' '\n'|sed -e 's/^"//' -e 's/"$//' -e 's/ /_/g'`
    OLD_IFS="$IFS" 
    IFS=","
    _array=($_data_columns)
    IFS=$OLD_IFS
    _sz_sh_data=`echo ${_array[0]}`
    #echo array is ${_array[@]}`
    if [ ${_sz_sh_data:0:2}x = ${_sh}x  ]
    then
        _mark_tp="1"
    else
        _mark_tp="2"
    fi
    
    OLD_IFS="$IFS"
    IFS="-"
    _array_date=(${_array[1]})
    IFS=$OLD_IFS
    _new_date=`echo ${_array_date[0]}${_array_date[1]}${_array_date[2]}`
#    echo length is ${#_array[@]}
    #if [ ${#_array[@]} -lt 19 ]
    #then
    #    _array[13]='0.0'
    #    _array[14]='0.0'
    #    _array[15]='0.0'
    #    _array[16]='0.0'
    #    _array[17]='0.0'
    #    _array[18]='0.0'
    #fi
    if [ ${#_array[14]} -lt 1 ]
    then
        _array[14]='0.0'
    fi
    if [ ${#_array[15]} -lt 1 ]
    then
        _array[15]='0.0'
    fi
    if [ ${#_array[18]} -lt 1 ]
    then
        _array[18]='0.0'
    fi
    _new_data=${_sz_sh_data:2}","${_new_date}","${_array[2]}","${_array[3]}","${_array[4]}","${_array[5]}","${_array[6]}","${_array[7]}","${_array[8]}","${_array[9]}","${_array[10]}","${_array[11]}","${_array[15]}","${_array[18]}
    echo ${_new_data} >> ${_table_name_prefix}${_mark_tp}"_"${_array_date[0]}".csv.new"
    #echo ${_new_data} >> ${_table_name_prefix}${_mark_tp}"_"${_array_date[0]}".new.csv"
    _new_table_name=${_table_name_prefix}${_mark_tp}"_"${_array_date[0]}
    #echo array date is ${_array_date[0]}
    _table_name="${_new_table_name}"
#    echo _table_name is $_table_name and _new_table_name is ${_new_table_name}
#done
#}
#make_data_for_file


# ensure table exists
echo "Creating table $_table_name and truncating if present"
#mysql -h $_db_host -P $_db_port -u $_db_user -p$_db_password << eof
#mysql -u $_db_user -p$_db_password -h$_db_host -P$_db_port $_db << eof
mysql -u $_db_user -p$_db_password -h$_db_host -P$_db_port $_db << eof

CREATE TABLE IF NOT EXISTS \`$_table_name\` (
  #code VARCHAR(32) NOT NULL,
  #date VARCHAR(32) NOT NULL,
  #open FLOAT NOT NULL DEFAULT '0',
  #high FLOAT NOT NULL DEFAULT '0',
  #low FLOAT NOT NULL DEFAULT '0',
  #close FLOAT NOT NULL DEFAULT '0',
  #change1 FLOAT NOT NULL DEFAULT '0',
  #volume BIGINT(20) NOT NULL DEFAULT '0',
  #money BIGINT(20) NOT NULL DEFAULT '0',
  #traded_market_value BIGINT(20) NOT NULL DEFAULT '0',
  #market_value BIGINT(20) NOT NULL DEFAULT '0',
  #turnover FLOAT NOT NULL DEFAULT '0',
  #adjust_price FLOAT NOT NULL DEFAULT '0',
  #report_type VARCHAR(32) NOT NULL DEFAULT '0',
  #report_date VARCHAR(32) NOT NULL DEFAULT '0',
  #PE_TTM FLOAT NOT NULL DEFAULT '0',
  #PS_TTM FLOAT NOT NULL DEFAULT '0',
  #PC_TTM FLOAT NOT NULL DEFAULT '0',
  #PB FLOAT NOT NULL DEFAULT '0',
  ##PRIMARY KEY (code,date)

  code VARCHAR(32) NOT NULL,
  date VARCHAR(32) NOT NULL,
  open FLOAT NOT NULL DEFAULT '0',
  high FLOAT NOT NULL DEFAULT '0',
  low FLOAT NOT NULL DEFAULT '0',
  close FLOAT NOT NULL DEFAULT '0',
  change1 FLOAT NOT NULL DEFAULT '0',
  volume BIGINT(20) NOT NULL DEFAULT '0',
  money BIGINT(20) NOT NULL DEFAULT '0',
  traded_market_value BIGINT(20) NOT NULL DEFAULT '0',
  market_value BIGINT(20) NOT NULL DEFAULT '0',
  turnover FLOAT NOT NULL DEFAULT '0',
  PE_TTM FLOAT NOT NULL DEFAULT '0',
  PB FLOAT NOT NULL DEFAULT '0',
  #PRIMARY KEY (code,date)

  id int(11) NOT NULL auto_increment,
  #code VARCHAR(32) NOT NULL,
  #date VARCHAR(32) NOT NULL,
  #open VARCHAR(32) NOT NULL DEFAULT '0',
  #high VARCHAR(32) NOT NULL DEFAULT '0',
  #low  VARCHAR(32 )NOT NULL DEFAULT '0',
  #close VARCHAR(32) NOT NULL DEFAULT '0',
  #change1 VARCHAR(32) NOT NULL DEFAULT '0',
  #volume VARCHAR(32) NOT NULL DEFAULT '0',
  #money VARCHAR(32) NOT NULL DEFAULT '0',
  #traded_market_value VARCHAR(32) NOT NULL DEFAULT '0',
  #market_value VARCHAR(32) NOT NULL DEFAULT '0',
  #turnover VARCHAR(32) NOT NULL DEFAULT '0',
  #adjust_price VARCHAR(32) NOT NULL DEFAULT '0',
  #report_type VARCHAR(32) NOT NULL DEFAULT '0',
  #report_date VARCHAR(32) NOT NULL DEFAULT '0',
  #PE_TTM VARCHAR(32) NOT NULL DEFAULT '0',
  #PS_TTM VARCHAR(32) NOT NULL DEFAULT '0',
  #PC_TTM VARCHAR(32) NOT NULL DEFAULT '0',
  #PB VARCHAR(32) NOT NULL DEFAULT '0',
  PRIMARY KEY (id,code,date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
TRUNCATE \`$_table_name\`
eof

done
}
make_data_for_file

## loop through header columns
#	for _header in ${_header_columns[@]}
#	do
##		echo "Creating a column for $_header"
#	# add column
##               mysql -u $_db_user -p$_db_password -h$_db_host -P$_db_port $_db --execute="alter table \`$_table_name\` add column \`$_header\` text"
#	done
#
done
#end of do line54 
#at this place,all of "new.csv" have already exists.
#and next, we will just run 'mysqlimport' the files into the mysql

# import csv into mysql
_csv_files=`ls -A *.csv.new`
#_csv_files=`ls -A *.new.csv`
for _csv_file in ${_csv_files[@]}
do
    
    # import csv into mysql
    echo "Importing into $_db.$_table_name"
#code,date,open,high,low,close,change1,volume,money,traded_market_value,market_value,turnover【11】,PE_TTM【15】,PB【18】
    _header_columns_string="code,date,open,high,low,close,change1,volume,money,traded_market_value,market_value,turnover,PE_TTM,PB"
    mysqlimport --fields-terminated-by="," --lines-terminated-by="\n" --columns="$_header_columns_string" -u $_db_user -p$_db_password -h$_db_host -P$_db_port $_db $_csv_directory/$_csv_file
    #mysqlimport --fields-terminated-by="\t" --lines-terminated-by="\n" --columns="$_header_columns_string" -u $_db_user -p$_db_password -h$_db_host -P$_db_port $_db $_csv_directory/$_csv_file
done
exit
