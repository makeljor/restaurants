import csv
from datetime import datetime
import time

import pymongo
from fuzzywuzzy import fuzz
from pymongo import MongoClient
import pprint


def clean_city():  # Data standardization for the city field
    ny_changes = 0
    for record in docs:  # For New York neighborhoods
        with open("ny_neighborhoods.txt", "r", newline="") as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        if record[3].lower() in content:
            record[3] = "new york"
            ny_changes += 1
        else:
            pass
    # print("NY changes:", ny_changes)        # print check the number of cities converted to --> New York

    la_changes = 0
    for record in docs:  # For Los Angeles neighborhoods
        with open("la_districts_neighborhoods.txt", "r", newline="") as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        if record[3].lower() in content:
            record[3] = "los angeles"
            la_changes += 1
        else:
            pass
    # print("LA changes:", la_changes)        # print check the number of cities converted to --> Los Angeles


def clean_phone():  # Data standardization for the phone field
    for record in docs:
        record[4] = record[4].replace("/", "-", 1)
        record[4] = record[4].replace(" ", "", 1)


def name_match():  # Determine a string similarity score for the name field
    loop1 = 0
    for record in docs:
        loop2 = 0
        results = []
        val1 = record[1]
        for a in docs:
            if loop1 == loop2:
                loop2 += 1
            else:
                val2 = a[1]
                result = fuzz.token_set_ratio(val1, val2)
                results.append(result)
                loop2 += 1
        loop1 += 1
        max_result = (max(results))
        record.append(max_result)


def address_match():  # Determine a string similarity score for the address field
    loop1 = 0
    for record in docs:
        loop2 = 0
        results = []
        val1 = record[2]
        for a in docs:
            if loop1 == loop2:
                loop2 += 1
            else:
                val2 = a[2]
                result = fuzz.token_sort_ratio(val1, val2)
                results.append(result)
                loop2 += 1
        loop1 += 1
        max_result = (max(results))
        record.append(max_result)


def phone_match():  # Determine a string similarity score for the phone field
    loop1 = 0
    for record in docs:
        loop2 = 0
        results = []
        val1 = record[4]
        for a in docs:
            if loop1 == loop2:
                loop2 += 1
            else:
                val2 = a[4]
                result = fuzz.token_sort_ratio(val1, val2)
                results.append(result)
                loop2 += 1
        loop1 += 1
        max_result = (max(results))
        record.append(max_result)


def city_match():  # Determine a string similarity score for the city field
    loop1 = 0
    for record in docs:
        loop2 = 0
        results = []
        val1 = record[3]
        for a in docs:
            if loop1 == loop2:
                loop2 += 1
            else:
                val2 = a[3]
                result = fuzz.token_set_ratio(val1, val2)
                results.append(result)
                loop2 += 1
        loop1 += 1
        max_result = (max(results))
        record.append(max_result)


def duplicate_index():  # Determine a duplicate index for all records based on string similarity of name, address and phone fields
    name_weight = 1
    address_weight = 1
    # city_weight = 1
    phone_weight = 1
    for record in docs:
        total = record[6] * name_weight + record[7] * address_weight + record[8] * phone_weight
        # total = record[6] * name_weight + record[7] * address_weight + record[8] * city_weight + record[9] * phone_weight
        record.append(total)


def write_output():  # Write the restaurant records back into a timestamped csv file
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y-%H-%M-%S")
    file = ("restaurant_output-" + dt_string + ".csv")
    with open(file, "w", newline="") as result_file:
        wr = csv.writer(result_file, dialect="excel")
        wr.writerows(docs)


def print_all():  # Print check all restaurant records
    for record in docs:
        print(record)


def dub_determination(dub_threshold):  # Determine which records are duplicates and which are non-duplicates
    with open("restaurants_gs_dubs.txt", "r", newline="") as f:
        gs_dubs = f.readlines()
    gs_dubs = [x.strip() for x in gs_dubs]

    dubs = []
    unique = []
    for record in docs:
        if record[(len(record) - 1)] >= dub_threshold:
            dubs.append(record[0])
        else:
            unique.append(record[0])
    # print("all dubs:", dubs)                    # all records identified as dubs
    # print("all unique:", unique)                # all records identified as unique
    # print("dub count:", len(dubs))              # identified dubs count
    # print("unique count:", len(unique))         # identified unique count

    return dubs, unique


def result_analysis(lists):
    dubs = lists[0]
    unique = lists[1]
    with open("restaurants_gs_dubs.txt", "r", newline="") as f:
        gs_dubs = f.readlines()
    gs_dubs = [x.strip() for x in gs_dubs]

    true_pos = 0  # identified dubs vs gs_dubs e.g. true positive
    false_pos = 0  # identified dubs vs gs_unique e.g. false positive
    for record in dubs:
        if record in gs_dubs:
            true_pos += 1
        else:
            false_pos += 1

    true_neg = 0  # identified unique vs gs_unique e.g. true neg
    false_neg = 0  # identified unique vs gs_dub e.g. false neg
    for record in unique:
        if record not in gs_dubs:
            true_neg += 1
        else:
            false_neg += 1

    precision = true_pos / (true_pos + false_pos)
    recall = true_pos / (true_pos + false_neg)
    average = (precision + recall) / 2

    print("true pos", true_pos)  # print check block
    print("false pos", false_pos)
    print("true neg", true_neg)
    print("false neg", false_neg)
    print("total records", (true_pos + false_pos + true_neg + false_neg))
    print("precision:", precision)
    print("recall:", recall)
    print("average:", average)


def download_restaurant_records(file):  # Download the restaurant records into the global variable docs = [] for use throughout the program
    with open(file, "r") as restaurant_file:
        reader = csv.reader(restaurant_file, delimiter="\t")
        next(reader, None)  # skip header line of TSV file

        docs
        for row in reader:
            docs.append(row)

def re_insert_header():
    header = ["id", "name", "address", "city", "phone", "type", "name_match", "address_match", "phone_match", "dub_index"]
    docs.reverse()
    docs.append(header)
    docs.reverse()

def mongo_import():
    try:
        connection = MongoClient()
        print("Connected successfully!!!")
    except:
        print("Could not connect to MongoDB")

    # database
    db = connection.database
    mongorecs = {}
    header = ["id", "name", "address", "city", "phone", "type", "name_match", "address_match", "phone_match", "dub_index"]
    for record in docs:
        mongorecs[header[0]] = record[0]
        # res2 = dict(zip(header, record))
        # res = dict(zip(header[0], record[0]))
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(mongorecs)

    # Created or Switched to collection names: restaurant_collection
    collection = db.restaurant_collection

    # Insert Data
    # rec_id1 = collection.insert_one(res)

    # Printing the data inserted
    # cursor = collection.find()
    # for record in cursor:
    #     print(record)

def mongo_import2():
    client = pymongo.MongoClient(
        "mongodb+srv://analytics:analytics-password@clusterjorim-6vzrd.mongodb.net/test?retryWrites=true&w=majority")
    db = client.restaurant


start_time = time.time()                            # Monitor program runtime
docs = []                                           # Global variable for restaurant records, used throughout program
download_restaurant_records("restaurants_min.tsv")      # Parameter is the external source file for restaurant records
clean_city()
clean_phone()
name_match()
address_match()
# city_match()                                      # Yields 100 for almost all cities, irrelevant for detection
phone_match()
duplicate_index()
result_analysis(dub_determination(270))            # Parameter is the duplicate threshold, should be in the 0-300 range
re_insert_header()
write_output()
# print_all()
# mongo_import()
mongo_import2()

print("--- %s seconds ---" % (time.time() - start_time))        # Monitor program runtime
