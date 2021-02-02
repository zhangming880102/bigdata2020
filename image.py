# encoding:utf-8
import numpy as np
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import cv2
def toEigen(x):
	img=np.asarray(bytearray(x[1]), dtype=np.uint8)
	img=cv2.imdecode(img,1)
	img=cv2.resize(img,(256,256))
	winSize = (64,64)
	blockSize = (64,64)
	blockStride = (8,8)
	cellSize = (16,16)
	nbins = 9
	hog = cv2.HOGDescriptor(winSize,blockSize,blockStride,cellSize,nbins)
	winStride = (8,8)
	padding = (8,8)
	img_hog = hog.compute(img, winStride, padding).reshape((-1,))
	file_name=x[0].split('/')[-1]
	return(file_name,img_hog)

def cosine(x,y):
	a=x[1]
	b=y.value[0][1]
	std1=np.sqrt(np.sum(a**2))
	std2=np.sqrt(np.sum(b**2))
	s=std1*std2
	if s==0:
		return 0
	return (x[0],np.sum(a*b)/s)

class Image:
	
	def __init__(self,sc,path):
		self.sc=sc
		rdd=sc.binaryFiles(path)
		self.rdd=rdd.map(toEigen)
		print('init done.')

	def predict(self,target,k):
		target_rdd_vec=self.rdd.filter(lambda x:x[0]==target).collect()
		bvec=self.sc.broadcast(target_rdd_vec)
		return self.rdd.filter(lambda x:x[0]!=target).map(lambda x:cosine(x,bvec)).takeOrdered(k, key=lambda x: -x[1])

def print_result(re):
	for r in re:
		print(r[0]+'\t'+'%.3f'%(r[1]))

if __name__ == '__main__':
	conf = SparkConf().setAppName("recommendation-server")
	sc = SparkContext(conf=conf)
	e=Image(sc,'image/*.jpg')
	print_result(e.predict('1.jpg',3))
