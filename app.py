from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
from recommend import ContentRecommend
from hotwords import HotWords
from sentiment import Sentiment
from image import Image
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<string:user_id>/topics/<int:count>", methods=["GET"])
def recommend_topics(user_id, count):
	logger.debug("User %s TOP ratings requested", user_id)
	top_topics = recommendation_engine.get_top_ratings(user_id,count)
	return json.dumps(top_topics,ensure_ascii=False)

@main.route("/<string:user_id>/users/<int:count>", methods=["GET"])
def recommend_users(user_id, count):
	logger.debug("User %s TOP ratings requested", user_id)
	top_users = content_recommend.predict(user_id,count)
	return json.dumps(top_users,ensure_ascii=False)
 
@main.route("/hotwords/<int:count>", methods=["GET"])
def hotwords(count):
	logger.debug("hotwords %d TOP ratings requested", count)
	re = hot_words.predictWords(count)
	return json.dumps(re,ensure_ascii=False)

@main.route("/hottopics/<int:count>", methods=["GET"])
def hottopics(count):
	logger.debug("User %d TOP ratings requested", count)
	re = hot_words.predictTopics(count)
	return json.dumps(re,ensure_ascii=False)

@main.route("/usersentiment/<string:user_id>", methods=["GET"])
def usersenti(user_id):
	logger.debug("User %s TOP ratings requested", user_id)
	re = sentiment.predictUser(user_id)
	return json.dumps(re,ensure_ascii=False)

@main.route("/topicsentiment/<string:topic_id>", methods=["GET"])
def topicsenti(topic_id):
	logger.debug("User %s TOP ratings requested", topic_id)
	re = sentiment.predictTopics(topic_id)
	return json.dumps(re,ensure_ascii=False)

@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
	logger.debug("User %s rating requested for movie %s", user_id, movie_id)
	ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
	return json.dumps(ratings)
 
 
@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
	# get the ratings from the Flask POST request object
	ratings_list = request.form.keys()[0].strip().split("\n")
	ratings_list = map(lambda x: x.split(","), ratings_list)
	# create a list with the format required by the negine (user_id, movie_id, rating)
	ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
	# add them to the model using then engine API
	recommendation_engine.add_ratings(ratings)
 
	return json.dumps(ratings)



 
 
def create_app(spark_context, dataset_path):
	global recommendation_engine 
	global content_recommend
	global hot_words
	global sentiment
	recommendation_engine = RecommendationEngine(spark_context, dataset_path)	
	content_recommend=ContentRecommend(spark_context,dataset_path)
	hot_words=HotWords(spark_context,dataset_path)
	sentiment=Sentiment(spark_context,dataset_path)
	img=Image(spark_context,dataset_path)
	app = Flask(__name__)
	app.config['JSON_AS_ASCII'] = False
	app.config['JSONIFY_MIMETYPE'] = "application/json;charset=utf-8"
	app.register_blueprint(main)
	return app 
