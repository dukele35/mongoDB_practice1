import json
import codecs
import sys
import pymongo

# ------------------------------------------
# FUNCTION 1: most_popular_cuisine
# ------------------------------------------
def most_popular_cuisine(my_collection):
    
    # total number of restaurants in New York
    total_restaurants = my_collection.count_documents({})
    
    # creating pipeline to aggregate data from my_collection to 
    # get the cuisine with the highest number of restaurants in New York
    most_popular_cuisine = list(my_collection.aggregate([
        # grouping and counting for each cuisine
        {"$group": {"_id": "$cuisine", "n_restaurants" : {"$sum": 1}}},
        # descending sorting for each of cuisine's number of restaurants
        {"$sort": {"n_restaurants": -1}},
        # getting the first result, i.e. the cuisine with the highest number of restaurants
        {"$limit": 1}
    ]))
    
    # getting the name of cuisine with the highest number of restaurants
    cuisine_name = most_popular_cuisine[0]['_id']
    
    # getting the number of restaurants for the most popular cuisine
    no_restaurants = most_popular_cuisine[0]['n_restaurants'] 
    
    # calculating the most popular cuisine's restanrant ratio with the total number of restaurants in New York
    ratio_cuisine = no_restaurants / total_restaurants * 100

    return cuisine_name, ratio_cuisine


# ------------------------------------------
# FUNCTION 2: ratio_per_borough_and_cuisine
# ------------------------------------------
def ratio_per_borough_and_cuisine(my_collection, cuisine):
    
    # list of boroughs in an ascending order
    boroughs = sorted(my_collection.distinct('borough'))
    
    # getting number of the "cuisine" restaurants for each borough
    cuisine_restaurants_by_boroughs = list(my_collection.aggregate([
        # filtering/matching the "cuisine" key with the most popular cuisine from "most_popular_cuisine()"
        {"$match": {"cuisine": cuisine}},

        # counting restaurants for each borough
        {"$group": {"_id": "$borough", "n_restaurants" : {"$sum": 1}}},

        # sorting results by boroughs' names in an ascending order
        {"$sort": {"_id": 1}}
    ]))
    
    # getting number of all-kinds-of restaurants for each borough
    all_restaurants_by_boroughs = list(my_collection.aggregate([
        # counting restaurants for each borough
        {"$group": {"_id": "$borough", "n_restaurants" : {"$sum": 1}}},

        # sorting results by boroughs' names in an ascending order
        {"$sort": {"_id": 1}}
    ]))
    
    
    # getting a list of number of the "most-popular-cuisine" restaurants for each borough 
    cuisine_boroughs = [i['n_restaurants'] for i in cuisine_restaurants_by_boroughs]

    # getting a list of number of all-kind-of restaurants for each borough 
    all_boroughs = [i['n_restaurants'] for i in all_restaurants_by_boroughs]
    
    # getting a list of ratio of the "most-popular-cuisine" restaurants to the number of all restaurants in each borough
    ratio = [cu/al for cu,al in zip(cuisine_boroughs, all_boroughs)]
    
    # the borough with the lowest ratio 
    potential_borough = boroughs[ratio.index(min(ratio))]
    
    # the lowest ratio
    lowest_ratio = min(ratio) * 100
    
    return potential_borough, lowest_ratio
    
    
    
# ------------------------------------------
# FUNCTION 3: ratio_per_zipcode
# ------------------------------------------
def ratio_per_zipcode(my_collection, cuisine, borough):

    # getting the top 5 zipcodes in the chosen borough for the most of all-kinds-of restaurants 
    top_5_most_rest = list(my_collection.aggregate([
        # filtering/matching the "borough" key with the chosen borough from the ratio_per_borough_and_cuisine() 
        {"$match": {"borough": borough}},

        # counting restaurants for all zipcodes in the chosen borough
        {"$group": {"_id": "$address.zipcode", "count": {"$sum": 1}}},

        # sorting the restaurant counts in an descending order, i.e. the greatest coming first and the fewest coming last
        {"$sort": {"count": -1}},

        # choosing the top 5 zipcodes with the most restaurants
        {"$limit": 5},

        # sorting zipcodes in a descending order
        # this is important for calculating the ratio of restaurants
        # of the chosen cuisine in the following steps
        {"$sort": {"_id": -1}} 
    ]))
    
    # list of the top 5 postcodes 
    top_5_zip = [i['_id'] for i in top_5_most_rest]
    
    # list of number of all restaurants for the top 5 postcodes
    no_rest_for_top_5 = [i["count"] for i in top_5_most_rest]
    
    # getting the number of the "cuisine" restaurants for each zipcode in the chosen borough
    cuis_rest_in_zip = list(my_collection.aggregate([

        # filtering/matching the "cuisine" key with the most popular cuisine from most_popular_cuisine()
        # and the "borough" key with the chosen borough from the ratio_per_borough_and_cuisine() 
        {"$match": {"cuisine": cuisine, "borough": borough}},

        # counting restaurants for all zipcodes in the chosen borough with the chosen cuisine
        {"$group": {"_id": "$address.zipcode", "count": {"$sum": 1}}},

        # sorting zipcodes in an descending order
        # this is important for calculating the ratio of restaurants
        # of the chosen cuisine in the following steps
        {"$sort": {"_id": -1}},

    ]))
    
    # list of the number of the "cuisine" restaurants in the top-5 zipcodes having the most of restaurants
    cuis_rest_in_top_5_zip = [i['count'] for i in cuis_rest_in_zip if i['_id'] in top_5_zip]
    
    # list of ratios of the "cuisine" restaurants to all restaurants for top 5 zipcodes
    ratio_top_5 = [cuis / top5 for cuis , top5 in zip(cuis_rest_in_top_5_zip, no_rest_for_top_5)]
    
    # the target zipcode
    target_zip = top_5_zip[ratio_top_5.index(min(ratio_top_5))]
    
    # the lowest ratio 
    ratio = min(ratio_top_5) * 100        

    return target_zip, ratio

# ------------------------------------------
# FUNCTION 4: best_restaurants
# ------------------------------------------
def best_restaurants(my_collection, cuisine, borough, zipcode):
    
    # getting the number of reviews for each restaurants 
    rest_review_count = list(my_collection.aggregate([

        # filtering/matching the "cuisine" key with the most popular cuisine from most_popular_cuisine(),
        # the "borough" key with the chosen borough from the ratio_per_borough_and_cuisine()
        # and "zipcode" with the chosen zipcodes from the ratio_per_zipcode()
        {"$match": {"address.zipcode": zipcode, "borough": borough,"cuisine": cuisine}},

        # drilling down the 'grade' field
        {"$unwind": "$grades"},

        # counting no. of reviews for each restaurants
        {"$group": {"_id": "$name", "count": {"$sum": 1}}}
    ]))
    
    # list of restaurants having at least 4 reviews
    rest_list = [i['_id'] for i in rest_review_count if i['count'] >= 4]
    
    # getting average review scores for the target restaurants
    target_rest = list(my_collection.aggregate([

        # getting info from restaurants having at least 4 reviews
        {"$match": {"name": {"$in": rest_list}}},

        # drilling down the 'grade' field
        {"$unwind": "$grades"},

        # calculating avarage review scores for each restaurants
        {"$group": {"_id": "$name", "average_score": {"$avg": "$grades.score"}}},

        # sorting restaurants having the average review scores in a descending order
        {"$sort": {"average_score": -1}},

        # getting the top three restaurants having the best average review scores
        {"$limit": 3}
    ]))
    
    # getting restaurants' names and their average review scores
    best, reviews = [i['_id'] for i in target_rest], [i['average_score'] for i in target_rest]
    
    return best, reviews


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------/
def my_main(database_name, collection_name):
    # 1. We set up the connection to mongos.exe allowing us to access to the cluster
    mongo_client = pymongo.MongoClient()

    # 2. We access to the desired database
    db = mongo_client.get_database(database_name)

    # 3. We access to the desired collection
    collection = db.get_collection(collection_name)

    # 4. What is the kind of cuisine with more restaurants in the city?
    (cuisine, ratio_cuisine) = most_popular_cuisine(collection)
    print("\n1. The kind of cuisine with more restaurants in the city is", cuisine, "(with a", ratio_cuisine, "percentage of restaurants of the city)")

    # 5. Which is the borough with smaller ratio of restaurants of this kind of cuisine?
    (borough, ratio_borough) = ratio_per_borough_and_cuisine(collection, cuisine)
    print("\n2. The borough with smaller ratio of restaurants of this kind of cuisine is", borough, "(with a", ratio_borough, "percentage of restaurants of this kind)")

    # 6. Which of the 5 biggest zipcodes of the borough has a smaller ratio of restaurants of the cuisine we are looking for?
    (zipcode, ratio_zipcode) = ratio_per_zipcode(collection, cuisine, borough)
    print("\n3. The zipcode of the borough with smaller ratio of restaurants of this kind of cuisine is zipcode =", zipcode, "(with a", ratio_zipcode, "percentage of restaurants of this kind)")

    # 7. Which are the best 3 restaurants (of the kind of cuisine we are looking for) of our zipcode?
    (best, reviews) = best_restaurants(collection, cuisine, borough, zipcode)
    print("\n4. The best three restaurants (of this kind of couisine) at these zipcode are:", best[0], "(with average reviews score of", reviews[0], "),", best[1], "(with average reviews score of", reviews[1], "),", best[2], "(with average reviews score of", reviews[2], ")")

    # 8. Close the client
    mongo_client.close()

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We get the input arguments
    my_database = "test"
    my_collection = "restaurants"

    # 2. We call to my_main
    my_main(my_database, my_collection)