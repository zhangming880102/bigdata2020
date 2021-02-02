import jieba
from snownlp import SnowNLP

user={}
word={}
topic={}

uf=open('user_sentim.csv','w')
wf=open('word_sentim.csv','w')
tf=open('topic_sentim.csv','w')

with open('w2.csv','r') as f:
	for line in f.readlines():
		arr=line.strip().split('\t')
		ts=jieba.cut(arr[1])
		s=SnowNLP(arr[1]).sentiments
		for t in ts:
			if t in word:
				word[t][0]=word[t][0]+s
				word[t][1]=word[t][1]+1
			else:
				word[t]=[s,1]
		t=arr[0]
		if t in user:
			user[t][0]=user[t][0]+s
			user[t][1]=user[t][1]+1
		else:
			user[t]=[s,1]

with open('t2.csv','r') as f:
	for line in f.readlines():
		arr=line.strip().split('\t')
		s=SnowNLP(arr[1]).sentiments
		t=arr[0]
		if t in topic:
			topic[t][0]=topic[t][0]+s
			topic[t][1]=topic[t][1]+1
		else:
			topic[t]=[s,1]

for u in user:
	uf.write(u+'\t'+'%.3f'%(user[u][0]/user[u][1])+'\n')
uf.close()
for u in word:
	wf.write(u+'\t'+'%.3f'%(word[u][0]/word[u][1])+'\n')
wf.close()
for u in topic:
	tf.write(u+'\t'+'%.3f'%(topic[u][0]/topic[u][1])+'\n')
tf.close()
