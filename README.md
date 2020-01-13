# restaurants
Data cleaning and duplicate detection program using python on a TSV file.
restaurants.py is the main program to run.

***Files contained***
Update README.md                readme
desktop.ini                     ???
la_districts_neighborhoods.txt  list of los angeles neighborhoods, pulled off of wikipedia for data standardization
ny_neighborhoods.txt            list of new york neighborhoods, pulled off of wikipedia for data standardization
restaurants.py                  main program
restaurants.tsv                 original collection of 864 restaurant records, for cleaning and duplicate detection
restaurants_gs_dubs.txt         the gold standard duplicates in a text file
restaurants_min.tsv             a cut down version of the 864 restaurant records, for quick test runs of the program

***Libraries and modules used***
import csv
from datetime import datetime
import time
import pymongo
from fuzzywuzzy import fuzz
from pymongo import MongoClient

The "dnspython" module must be installed to use mongodb+srv:// URIs

***NOTES***
MongoDB Atlas cluster settings and credentials in mongo_import() function must be inserted to upload document to MongoDB.

***Program Architecture***
start_time = time.time()                            		# Monitor program runtime. 1/2
docs = []                                           		# Global variable for restaurant records, used throughout program.
mongorecs = {}                                      		# Global variable for records in dictionary format, for MongoDB.
download_restaurant_records("restaurants.tsv")      		# Parameter is the external source file for restaurant records. Downloads data from external file to docs. Removes header row.
clean_city()                                        		# Clean up of the city values.
clean_phone()                                       		# Clean up of the phone values.
name_match()                                        		# Determine string similarity of name value.
address_match()                                     		# Determine string similarity of address value.
# city_match()                                      		# Determine string similarity of city value. Yields score of 100 for almost all records, irrelevant for detection. Do not uncomment as will cause error.
phone_match()                                       		# Determine string similarity of phone value.
duplicate_index()                                   		# Determine overall similarity of record.
result_analysis(dub_determination(270))             		# Parameter is the duplicate threshold, should be in the 0-300 range. Flags duplicates. Prints important analytical information about results.
re_insert_header()                                  		# Inserts the header row back into the docs
# write_output()                                    		# Writes the documents back into an output CSV file - used extensively for analysis and optimization
dictionary_conversion()                             		# Convert docs to dictionary for MongoDB
mongo_import()                                      		# Upload documents into MongoDB
# print_all()                                       		# For print checking
print("--- %s seconds ---" % (time.time() - start_time))	# Monitor program runtime. 2/2


***Prerequisites***
See "Libraries and modules used"

***Installing***
See notes (MongoDB Atlas cluster setup)
Run the restaurants.py.

***Versioning***
This is the final version :)

***Authors***
Jori Maekelae

***License***
Free to use

***Acknowledgments***
Great course, interesting stuff, lots of fun!
