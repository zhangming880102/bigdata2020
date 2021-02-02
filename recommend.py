import os
import jieba
from pyspark import SparkConf, SparkContext
import math

def cut(x):
	return jieba.cut(x[1])


def sort(x):
	return x


def wordvec(x,vocabulary,vocabulary_len):
	vec=[0]*(vocabulary_len.value+1)
	tokens=jieba.cut(x[1])
	for token in tokens:
		idx=vocabulary.value[token]
		vec[idx]=vec[idx]+1
	return (x[0],vec)

def wordvec_sum(a,b,vocabulary_len):
	for i in range(vocabulary_len.value+1):
		a[i]=a[i]+b[i]
	return a


def calc_corr(x, bvec):
	b=bvec.value[0][1]
	a=x[1]
	a_avg = sum(a) / len(a)
	b_avg = sum(b) / len(b)
	cov_ab = sum([(x - a_avg) * (y - b_avg) for x, y in zip(a, b)])
	sq = math.sqrt(sum([(x - a_avg) ** 2 for x in a]) * sum([(x - b_avg) ** 2 for x in b]))

	corr_factor = cov_ab / sq

	return (x[0],corr_factor)



class ContentRecommend:

	def __init__(self,sc,dataset_path):
		self.sc=sc
		rdd=sc.textFile(os.path.join(dataset_path,'wcontent.csv'))
		rdd=rdd.map(lambda x:x.split('\t'))
		self.mydict=rdd.flatMap(cut).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).map(lambda x:x[0]).sortByKey(sort).zipWithIndex().collectAsMap()
		self.dict_len=max(list(self.mydict.values()))
		vocabulary=sc.broadcast(self.mydict)
		vocabulary_len=sc.broadcast(self.dict_len)
		self.user_word_vec=rdd.map(lambda x:wordvec(x,vocabulary,vocabulary_len)).reduceByKey(lambda a,b:wordvec_sum(a,b,vocabulary_len))
	def predict(self,user_id,limits):
		user_rdd_vec=self.user_word_vec.filter(lambda x:x[0]==user_id).collect()
		bvec=self.sc.broadcast(user_rdd_vec)
		result=self.user_word_vec.filter(lambda x:x[0]!=user_id).map(lambda x: calc_corr(x,bvec)).takeOrdered(limits, key=lambda x: -x[1])
		return result

def print_result(re):
	for r in re:
		print(r[0]+'\t'+'%.3f'%(r[1]))

if __name__ == '__main__':
	conf = SparkConf().setAppName("recommendation-server")
	sc = SparkContext(conf=conf)
	e=ContentRecommend(sc,'./')
	print_result(e.predict('6473015115',50))

