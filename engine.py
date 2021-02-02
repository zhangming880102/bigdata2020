import os

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
	nratings = len(ID_and_ratings_tuple[1])
	return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class RecommendationEngine:
	"""A recommendation engine
	"""

	def __count_and_average_ratings(self):
		"""Updates the ratings counts from
		the current data self.ratings_RDD
		"""
		logger.info("Counting ratings...")
		weibo_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
		weibo_ID_with_avg_ratings_RDD = weibo_ID_with_ratings_RDD.map(get_counts_and_averages)
		self.weibo_rating_counts_RDD = weibo_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))


	def __train_model(self):
		"""Train the ALS model with the current dataset
		"""
		logger.info("Training the ALS model...")
		self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
							   iterations=self.iterations, lambda_=self.regularization_parameter)
		logger.info("ALS model built!")


	def __predict_ratings(self, user_and_weibo_RDD):

		predicted_RDD = self.model.predictAll(user_and_weibo_RDD)
		predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
		predicted_rating_title_and_count_RDD = \
			predicted_rating_RDD.join(self.weibo_titles_RDD).join(self.weibo_rating_counts_RDD)
		predicted_rating_title_and_count_RDD = \
			predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1],r[1][0][0]))
		
		return predicted_rating_title_and_count_RDD
	
	def add_ratings(self, ratings):
		"""Add additional ratings in the format (user_id, m_id, rating)
		"""
		# Convert ratings to an RDD
		new_ratings_RDD = self.sc.parallelize(ratings)
		# Add new ratings to the existing ones
		self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
		# Re-compute ratings count
		self.__count_and_average_ratings()
		# Re-train the ALS model with the new ratings
		self.__train_model()
		
		return ratings

	def get_ratings_for_weibo_ids(self, user_id, weibo_ids):

		requested_weibo_RDD = self.sc.parallelize(weibo_ids).map(lambda x: (user_id, x))
		# Get predicted ratings
		ratings = self.__predict_ratings(requested_weibo_RDD).collect()

		return ratings
	
	def get_top_ratings(self, user_id, count):
		
		user_id=self.user_dict[user_id]
		user_unrated_topic_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
												 .map(lambda x: (user_id, x[1])).distinct()
		ratings = self.__predict_ratings(user_unrated_topic_RDD).takeOrdered(count, key=lambda x: -x[1])

		return ratings

	def load_dict(self,dict_path):
		d={}
		with open(dict_path,'r') as f:
			for line in f.readlines():
				arr=line.split(',')
				d[arr[0]]=int(arr[1])

			f.close()
		return d

	def __init__(self, sc, dataset_path):


		logger.info("Starting up the Recommendation Engine: ")

		self.sc = sc
		self.user_dict=self.load_dict(os.path.join(dataset_path,'user_dict.csv'))
		# Load ratings data for later use
		logger.info("Loading Ratings data...")
		ratings_file_path = os.path.join(dataset_path, 'rating.csv')
		ratings_raw_RDD = self.sc.textFile(ratings_file_path)
		ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
		self.ratings_RDD = ratings_raw_RDD.map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
		# Load  data for later use
		logger.info("Loading data...")
		weibo_file_path = os.path.join(dataset_path, 'topic.csv')
		weibo_raw_RDD = self.sc.textFile(weibo_file_path)
		weibo_raw_data_header = weibo_raw_RDD.take(1)[0]
		self.weibo_RDD = weibo_raw_RDD.map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[1])).cache()
		self.weibo_titles_RDD = self.weibo_RDD.map(lambda x: (int(x[0]),x[1])).cache()
		# Pre-calculate ratings counts
		self.__count_and_average_ratings()

		# Train the model
		self.rank = 8
		self.seed = 5
		self.iterations = 10
		self.regularization_parameter = 0.1
		self.__train_model() 

def print_result(re):
	for r in re:
		print(r[0]+'\t'+'%.3f'%(r[1]))

if __name__== "__main__":
	conf = SparkConf().setAppName("recommendation-server")
	sc = SparkContext(conf=conf)
	engine=RecommendationEngine(sc,'./')
	top_ratings = engine.get_top_ratings('6495364288',50)
	print_result(top_ratings)
