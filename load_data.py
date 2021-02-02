import os

def trans_to_als_data(data_path,out_dir):
	rating_outf=open(os.path.join(out_dir,'tot_rating.csv'),'w')
	topic_outf=open(os.path.join(out_dir,'tot_topic.csv'),'w')
	dict_outf=open(os.path.join(out_dir,'tot_user_dict.csv'),'w')
	tc_outf=open(os.path.join(out_dir,'topiccontent.csv'),'w')
	topics={}
	topics_count=0
	user_topic_count={}
	with open(data_path,'r',encoding='utf-8') as f:
		for line in f.readlines():
			line=line.strip('\n')
			array=line.split(',')
			if len(array)!=17:
				continue
			t=array[8]
			t=t.strip()
			if len(t)==0:
				continue
			uid=array[2]
			if not t in topics:
				topics[t]=topics_count
				topics_count=topics_count+1
			tid=topics[t]
			tc_outf.write(str(tid)+'\t'+array[4]+'\n')
			if uid in user_topic_count:
				uidt=user_topic_count[uid]
				if tid in uidt:
					uidt[tid]=uidt[tid]+1
				else:
					uidt[tid]=1
				user_topic_count[uid]=uidt
			else:
				uidt={}
				uidt[tid]=1
				user_topic_count[uid]=uidt
		f.close()
	tc_outf.close()
	user_count=0
	user_dict={}
	for uid in user_topic_count:
		uidt=user_topic_count[uid]
		if not uid in user_dict:
			user_dict[uid]=user_count
			dict_outf.write(str(uid)+','+str(user_count)+'\n')
			user_count=user_count+1
		for tid in uidt:
			rating_outf.write(str(user_dict[uid])+','+str(tid)+','+str(uidt[tid])+',0\n')

	for t in topics:
		topic_outf.write(str(topics[t])+','+t+'\n')

	rating_outf.close()
	topic_outf.close()
	dict_outf.close()
def trans_to_content_data(data_path,out_dir):
	content_outf=open(os.path.join(out_dir,'weibocontent.csv'),'w')
	name_outf=open(os.path.join(out_dir,'tot_user_name.csv'),'w')
	with open(data_path,'r',encoding='utf-8') as f:
		for line in f.readlines():
			line=line.strip('\n')
			array=line.split(',')
			if len(array)!=17:
				continue
			name_outf.write(array[2]+'\t'+array[3]+'\n')
			content_outf.write(str(array[2])+'\t'+array[4]+'\n')
		f.close()
	content_outf.close()

if __name__=="__main__":
	trans_to_als_data('content.csv','./')
	#trans_to_content_data('content.csv','./')
