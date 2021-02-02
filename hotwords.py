import os
import jieba
from pyspark import SparkConf, SparkContext
import math

def cut(x):
	return jieba.cut(x[1])


def sort(x):
	return x

def stopwords(path):
	s={}
	with open(path,'r') as f:
		for line in f.readlines():
			s[line.strip()]=1
		f.close()
	return s

def filt(data,s):
	re={}
	for d in data:
		if not d in s:
			re[d]=data[d]
	return re

	
class HotWords:

	def __init__(self,sc,dataset_path):
		self.sc=sc
		rdd=sc.textFile(os.path.join(dataset_path,'weibocontent.csv'))
		rdd=rdd.map(lambda x:x.split('\t'))
		self.mydict=rdd.flatMap(cut).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).collectAsMap()
		stopw=stopwords('stopwords.txt')
		data=filt(self.mydict,stopw)
		self.data=sorted(data.items(),key=lambda kv:(kv[1],kv[0]),reverse=True)
		
		trdd=sc.textFile(os.path.join(dataset_path,'tot_rating.csv'))
		self.trdd=trdd.map(lambda x:x.split(',')).map(lambda x:(x[1],1)).reduceByKey(lambda a,b:a+b)
		prdd=sc.textFile(os.path.join(dataset_path,'tot_topic.csv'))
		prdd=prdd.map(lambda x:x.split(',')).map(lambda x:(x[0],x[1]))
		self.trdd=self.trdd.join(prdd).map(lambda x:(x[0],x[1][0],x[1][1]))
		print('init done.')
	def predictWords(self,limits):
		return self.data[0:limits]

	def predictTopics(self,limits):
		return self.trdd.takeOrdered(limits,key=lambda x: -x[1])

def print_result2(re):
	for r in re:
		print(r[0]+'\t'+r[2]+'\t'+'%d'%(r[1]))

def print_result(re):
	for r in re:
		print(r[0]+'\t'+str(r[1]))
if __name__ == '__main__':
	conf = SparkConf().setAppName("recommendation-server")
	sc = SparkContext(conf=conf)
	e=HotWords(sc,'./')
	print_result(e.predictWords(50))

