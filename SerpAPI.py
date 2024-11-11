#!/usr/bin/env python
# coding: utf-8
from serpapi import GoogleSearch
import googlemaps
import csv
import pandas as pd
import numpy as np
import aiohttp
import asyncio
import pandas as pd

from tqdm.notebook import tqdm
tqdm.pandas()

serp_api_key='8a8e3b429e2fc84b99fe82f5093a4644947098bd07f9ec44259dca8f8de4e85c'

# First API call to capture Google My Business Page
def search_results(google_search):
    params = {
      "engine": "google",
      "q": google_search,
      "api_key": serp_api_key,
      "hl":"en",
      "gl":"us", 
      "device":"desktop",
      "google_domain":"google.com"  
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    return results


# Functions to pull various information from the GMB
def store_search_id(results):  
    try: 
        return results['search_metadata']['id']
    except:
        return "No searchID generated"

def get_place_id(results):  
    try: 
        return results['knowledge_graph']['place_id']
    except:
        return "No place found"

def get_place_address(results):
    try:
        return results['knowledge_graph']['address']
    except:
        return "No place found"

def get_shop_name(results):
    try: 
        return results['knowledge_graph']['title']
    except:
        return "No place found"

def get_search_link(results):
    try: 
        return results['knowledge_graph']['knowledge_graph_search_link']
    except:
        return "No place found"

def price_lookup(results):
    try:
        return results['knowledge_graph']['price']
    except:
        return "No price found"

def googleclassification_lookup(results):
    try:
        return results['knowledge_graph']['type']
    except:
        return "No Google Classification found"
    
def reservation_type(results):
    try:
        return results['knowledge_graph']['reservation_providers'][0]['name']
    except:
        return "No reservation provider found"

def is_perm_closed(results):
    try:
        return results['knowledge_graph']['permanently_closed']
    except:
        return "No closure banner found"


# 2nd API Call to Reviews API to pull the first two pages of reviews (this counts as 2 API Calls for each page pulled)

def fetch_reviews(place_id, api_key, max_reviews=18):
    all_reviews = []
    next_page_token = None
    while len(all_reviews) < max_reviews:
        params = {
            "engine": "google_maps_reviews",
            "place_id": place_id,
            "api_key": api_key,
            "hl": "en",  
            "sort_by": "qualityScore"
        }

        if next_page_token:
            params["next_page_token"] = next_page_token

        search = GoogleSearch(params)
        results = search.get_dict()

        if "reviews" in results:
            all_reviews.extend(results["reviews"])

        if "serpapi_pagination" in results and "next_page_token" in results["serpapi_pagination"]:
            next_page_token = results["serpapi_pagination"]["next_page_token"]
        else:
            break

    return all_reviews[:18]



# Function to perform calcs on how many reviews are scraped and how many of those reviews contain pizza keywords
def review_audit(place_id,max_reviews):

    reviews = fetch_reviews(place_id, serp_api_key, max_reviews)

# Print out the reviews
    review_count=0
    reviews_with_pizza=0
    reviews_with_alcohol = 0
    pizza_keywords = ['pizza', 'pie', 'pizzeria', 'slice']
    alcohol_keywords = ['liquor', 'whisky', 'cocktail', 'wine', 'alcohol',' gin ', 'tequila', 'scotch', 'bourbon']
    for review in reviews:
        try:
            if len(review['snippet']) > 1:
                review_count += 1

                if any(keyword in review['snippet'].lower() for keyword in pizza_keywords):
                    reviews_with_pizza += 1
                
                if any(keyword in review['snippet'].lower() for keyword in alcohol_keywords):
                    reviews_with_alcohol += 1
        except:
            review_count += 0
    return [review_count, reviews_with_pizza, reviews_with_alcohol]


# Wrapper function to call previously defined functions and outputs results to a csv
def process_csv(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    df['results']=df['Google Search'].progress_apply(search_results)
    df['searchID'] = df['results'].apply(store_search_id)
    df['PlaceID'] = df['results'].apply(get_place_id)
    df['Shop Name']=df['results'].apply(get_shop_name)
    df['Address']=df['results'].apply(get_place_address)
    df['output'] = df.progress_apply(lambda row:review_audit(row['PlaceID'],18),axis=1)
    df['Review_counts']= df['output'].apply(lambda x: pd.Series(x))[0]
    df['Reviews_with_pizza']= df['output'].apply(lambda x: pd.Series(x))[1]
    df['Reviews_with_alcohol']= df['output'].apply(lambda x: pd.Series(x))[2]
    df['TP_review_audit']=(df['Reviews_with_pizza']/df['Review_counts']).apply(lambda x: f"{int(np.nan_to_num(x * 100))}%")
    df['Reservation Provider']=df['results'].apply(reservation_type)
    df['Price']=df['results'].apply(price_lookup)
    df['Google Classification']=df['results'].apply(googleclassification_lookup)
    df['Permanently closed']=df['results'].apply(is_perm_closed)
    df.drop(['results','output'], axis=1,inplace=True)
    df.to_csv(file_path[:-4]+" - output.csv", index=False)
    return df





# **Use the following for processing large API requests**


# Asynchronous function to fetch API results from Google Places API
SERP_API_URL="https://serpapi.com/search"

async def async_search_results(session, google_search):
    params = {
        'q': google_search,
        'api_key': serp_api_key,
        'engine': 'google',
        "hl":"en",
        "gl":"us", 
        "device":"desktop"
    }
    async with session.get(SERP_API_URL, params=params) as response:
        data = await response.json()
        return data

# Asynchronous function to handle multiple requests
async def process_async_search_results(df):
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Use tqdm to iterate over queries with progress bar
        for query in tqdm(df['Google Search'], total=len(df['Google Search'])):
            tasks.append(async_search_results(session, query))
        return await asyncio.gather(*tasks, return_exceptions=True)



# Asynchronous function to fetch reviews from Google Reviews API
async def async_fetch_reviews(place_id, api_key, max_reviews=18):
    all_reviews = []
    next_page_token = None
    review_search_ids=[]

    async with aiohttp.ClientSession() as session:
        while len(all_reviews) < max_reviews:
            params = {
                "engine": "google_maps_reviews",
                "place_id": place_id,
                "api_key": api_key,
                "hl": "en",  # Language (optional),
                "sort_by": "qualityScore"
            }

            if next_page_token:
                params["next_page_token"] = next_page_token

            async with session.get("https://serpapi.com/search", params=params) as response:
                results = await response.json()
                review_search_ids.append(results['search_metadata']['id'])

            if "reviews" in results:
                all_reviews.extend(results["reviews"])

            if "serpapi_pagination" in results and "next_page_token" in results["serpapi_pagination"]:
                next_page_token = results["serpapi_pagination"]["next_page_token"]
            else:
                break

    return all_reviews,review_search_ids


async def apply_review_audit(row, max_reviews):
    reviews,review_search_ids = await async_fetch_reviews(row['PlaceID'], serp_api_key, max_reviews)

    # Process reviews directly and return the results
    review_count = 0
    reviews_with_pizza = 0
    reviews_with_alcohol = 0
    pizza_keywords = ['pizza', 'pie', 'pizzeria', 'slice']
    alcohol_keywords = ['liquor', 'whisky', 'cocktail', 'wine', 'alcohol',' gin ', 'tequila', 'scotch', 'bourbon']
    for review in reviews:
        try:
            if len(review['snippet']) > 1:
                review_count += 1

                if any(keyword in review['snippet'].lower() for keyword in pizza_keywords):
                    reviews_with_pizza += 1
                
                if any(keyword in review['snippet'].lower() for keyword in alcohol_keywords):
                    reviews_with_alcohol += 1
        except:
            review_count += 0
    return [review_count, reviews_with_pizza, reviews_with_alcohol,review_search_ids]

async def apply_review_audit_async(df,max_reviews):
    tasks = []
    for index, row in tqdm(df.iterrows(), total=len(df)):
        task = asyncio.create_task(apply_review_audit(row,max_reviews))
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    return results


# **Run Searches Function and Reviews Function separately (to help with debugging / be able to save in case of error later) in batches of 5000 max**

# 1) Run Searches Function to retrieve API reponse for each shop


# Example usage
df = pd.read_csv('Batch 3.csv')
df['api_response'] = await process_async_search_results(df)


# 2) Run functions on the API response and store in the dataframe


df['PlaceID'] = df['api_response'].apply(get_place_id)
df['searchID'] = df['api_response'].apply(store_search_id)
df['Shop Name']=df['api_response'].apply(get_shop_name)
df['Address']=df['api_response'].apply(get_place_address)
df['Price']=df['api_response'].apply(price_lookup)
df['Google Classification']=df['api_response'].apply(googleclassification_lookup)
df['Reservation Provider']=df['api_response'].apply(reservation_type)



# 3) Run the Review Function and store the results in a new column called 'Review_Audit'


df['Review_Audit'] = await apply_review_audit_async(df,18)


# 4) Run cleanup functions on the output of the main Review Function 


df['Review_counts']= df['Review_Audit'].apply(lambda x: pd.Series(x))[0]
df['Reviews_with_pizza']= df['Review_Audit'].apply(lambda x: pd.Series(x))[1]
df['Reviews_with_alcohol']= df['Review_Audit'].apply(lambda x: pd.Series(x))[2]
df['Review_Search_IDs']= df['Review_Audit'].apply(lambda x: pd.Series(x))[3]
df['Pizza_Focus']=(df['Reviews_with_pizza']/df['Review_counts']).apply(lambda x: f"{int(np.nan_to_num(x * 100))}%")
df['Alcohol_Focus']=(df['Reviews_with_alcohol']/df['Review_counts']).apply(lambda x: f"{int(np.nan_to_num(x * 100))}%")


df.drop(['api_response','Email','Review_Audit'], axis=1,inplace=True)

