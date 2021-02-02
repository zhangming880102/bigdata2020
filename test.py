import jieba
from snownlp import SnowNLP


with open('te.csv','r') as f:
	for line in f.readlines():
		arr=line.strip().split('\t')
		s=SnowNLP(arr[1]).sentiments
