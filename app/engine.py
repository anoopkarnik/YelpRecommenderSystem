
# coding: utf-8

# In[169]:

import os
from pyspark.mllib.recommendation import ALS
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# In[214]:

class RecommendationEngine:
    """A yelp recommendation engine
    """
    def count_and_average_ratings(self):
        """Updates the movies ratings counts from 
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        business_id_with_ratings = self.review_final.map(lambda x: (x[1], x[2])).groupByKey()
        business_id_with_avg_ratings = business_id_with_ratings.map(get_counts_and_averages)
        self.business_rating_counts_RDD = business_id_with_avg_ratings.map(lambda x: (x[0], x[1][0],x[1][1]))
        return self.business_rating_counts_RDD
    
    def train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(self.review_final, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")
    def __init__(self,sc):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        import numpy as np
        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc
		
        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        review_base1= sc.textFile('/home/anoop/yelp_recommender/yelp_dataset/review.json')
        business_base1 = sc.textFile('/home/anoop/yelp_recommender/yelp_dataset/business.json')
        user_base1 = sc.textFile('/home/anoop/yelp_recommender/yelp_dataset/user.json')
        review_base2 = review_base1.map(json.loads)
        business_base2 = business_base1.map(json.loads)
        user_base2 = user_base1.map(json.loads)
        review_base3 = review_base2.map(lambda x : (x['user_id'],x['business_id'],x['stars']))
        user_base3 = user_base2.map(lambda x : (x['user_id'],x['name']))
        business_base3 = business_base2.map(lambda x : (x['business_id'],x['name'],x['review_count'],x['city'],x['categories']))
        user_base4 = user_base3.zipWithIndex()
        business_base4 = business_base3.zipWithIndex()
        user_base5 = user_base4.map(lambda x : (x[0][0],x[0][1],x[1]))
        business_base5 = business_base4.map(lambda x : (x[0][0],x[0][1],x[0][2],x[0][3],x[0][4],x[1]))
        business_base6 = business_base5.map(lambda x : ((x[0]),(x[1],x[2],x[3],x[4],x[5])))
        review_base4 = review_base3.map(lambda x : ((x[1]),(x[0],x[2])))
        review_base5 = business_base6.join(review_base4)
        review_base6 = review_base5.map(lambda x : ((x[1][1][0]),(x[1][0][1],x[1][1][1])))
        user_base6 = user_base5.map(lambda x : ((x[0]),(x[1],x[2])))
        review_base7 = review_base6.join(user_base6)
        review_base_final =review_base7.map(lambda x : (x[1][1][1],x[1][0][0],x[1][0][1]))
        self.review_final =review_base_final.map(lambda x : (x[0],x[1],x[2])).cache()

        self.business_name = business_base6.map(lambda x: (int(x[1][4]),x[1][0])).cache()
        self.business_details = business_base6.map(lambda x : (int(x[1][4]),x[1][0],x[1][2],x[1][3]))
        self.count_and_average_ratings()
        user_count = user_base1.count()
        business_count = business_base1.count()
        total_count = user_count*business_count
               
        
            
            
        # Pre-calculate movies ratings counts      

        # Train the model
        self.rank = 1
        self.seed = 5L
        self.iterations = 12
        self.regularization_parameter = 0.1
        self.train_model() 


# In[215]:

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


# In[216]:

def add_ratings(self, ratings):
    """Add additional movie ratings in the format (user_id, movie_id, rating)
    """
    # Convert ratings to an RDD
    new_ratings_RDD = self.sc.parallelize(ratings)
    # Add new ratings to the existing ones
    self.review_final = self.review_final.union(new_ratings_RDD)
    # Re-compute movie ratings count
    # Re-train the ALS model with the new ratings
    self.count_and_average_ratings()
    self.train_model()

    return ratings

# Attach the function to a class method
RecommendationEngine.add_ratings = add_ratings


# In[223]:

def predict_ratings(self, user_and_business_ids):
    """Gets predictions for a given (userID, movieID) formatted RDD
    Returns: an RDD with format (movieTitle, movieRating, numRatings)
    """
    predicted_RDD = self.model.predictAll(user_and_business_ids)
    predicted_rating_RDD = predicted_RDD.map(lambda x: (x[1], x[2]))
    predicted_rating_title_and_count_RDD = predicted_rating_RDD.join(self.business_name).join(self.business_rating_counts_RDD)
    predicted_rating_title_and_count_RDD = predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    
    return predicted_rating_title_and_count_RDD
    


# In[229]:

def get_top_ratings(self, user_id, business_count):
    user_unrated_business = self.review_final.filter(lambda rating: not rating[1]==user_id).map(lambda x: (user_id, x[0])).distinct()
    # Get predicted ratings
    ratings = self.predict_ratings(user_unrated_business).filter(lambda x : x[2] >=25).takeOrdered(business_count, key=lambda x: -x[1])

    return ratings


# In[230]:

RecommendationEngine.predict_ratings = predict_ratings
RecommendationEngine.get_top_ratings = get_top_ratings


# In[226]:

def get_ratings_for_business_ids(self, user_id, business_ids):
    """Given a user_id and a list of movie_ids, predict ratings for them 
    """
    requested_business_id = self.sc.parallelize(business_ids).map(lambda x: (user_id, x))
    # Get predicted ratings
    ratings = self.predict_ratings(requested_business_id).collect()
    
    return ratings

# Attach the function to a class method
RecommendationEngine.get_ratings_for_business_ids = get_ratings_for_business_ids

def popularity_model(self,location):
    business_details = self.business_details.keyBy(lambda x :x[0])
    ratings_base1  =  self.business_rating_counts_RDD.filter(lambda x : x[1] >=25).map(lambda x : (x[0],x[2])).keyBy(lambda x : x[0]).join(business_details)
    ratings_base2 = ratings_base1.map(lambda x : (x[0],x[1][0][1],x[1][1][1],x[1][1][2],x[1][1][3]))
    ratings = ratings_base2.filter(lambda x :x[3] == location).takeOrdered(10,key = lambda x : -x[1])
    return ratings
RecommendationEngine.popularity_model = popularity_model
    



