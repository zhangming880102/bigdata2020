import os
import jieba
from pyspark import SparkConf, SparkContext
import math
from snownlp import SnowNLP

def cut(x):
	xx=jieba.cut(x[1])
	dd=[]
	s=SnowNLP(x[1]).sentiments
	for xxx in xx:
		dd.append((xxx,[s,1]))
	return dd
def senti(x):
	return (x[0],[SnowNLP(x[1]).sentiments,1])

def sort(x):
	return x

def meansenti(a,b):
	return (a[0]+b[0],a[1]+b[1])
	
class Sentiment:

	def __init__(self,sc,dataset_path):
		self.sc=sc
		rdd=sc.textFile(os.path.join(dataset_path,'wcontent.csv'))
		rdd=rdd.map(lambda x:x.split('\t'))
		self.worddata=rdd.flatMap(cut).reduceByKey(meansenti).map(lambda x:(x[0],x[1][0]/x[1][1]))
		
		rdd2=sc.textFile(os.path.join(dataset_path,'te.csv'))
		rdd2=rdd2.map(lambda x:x.split('\t'))
		self.topicdata=rdd2.map(senti).reduceByKey(meansenti).map(lambda x:(x[0],x[1][0]/x[1][1]))

		self.userdata=rdd.map(senti).reduceByKey(meansenti).map(lambda x:(x[0],x[1][0]/x[1][1]))
		print('init done.')
	def predictUser(self,user_id):
		return self.userdata.filter(lambda x:x[0]==user_id).collect()

	def predictWord(self,word):
		return self.worddata.filter(lambda x:x[0]==word).collect()
		
	def predictTopics(self,topic_id):
		return self.topicdata.filter(lambda x:x[0]==topic_id).collect()

def print_result(re):
	for r in re:
		print(r[0]+'\t'+'%.3f'%(r[1]))

if __name__ == '__main__':
	conf = SparkConf().setAppName("sentiment-server")
	sc = SparkContext(conf=conf)
	e=Sentiment(sc,'./')
	print_result(e.predictUser('6473015115'))

