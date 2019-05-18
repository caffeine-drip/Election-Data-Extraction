import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit

import os
from os import walk
import glob

sc = SparkContext('local','ETL_Markpu')
sqlContext = SQLContext(sc)



##Code
def Etl_job2(inpath,outpath):
    #
    #print("\n\n\n\n")
    #print("\nINDATA path=====>",inpath,"\nOUTDATA path=====>",outpath)
    #print("\n\n")
    file_location = inpath
    f_name=os.path.basename(file_location)
    fname_list=f_name.split('_')
    mid=fname_list[0]+'_'+fname_list[1].split('-')[0]+'_'+fname_list[2].split('-')[-1]
    df = sqlContext.read.format("csv").option("inferSchema",True).option("header",True).option("delimiter","|").load(file_location)
    
    ##Get count of unique title and make a list
    temp=df.groupBy('Contest Title').count()
    c_title = [str(row['Contest Title']) for row in temp.collect()]
    
    ##Generate unique file for each title
    for title in c_title:
        #print("\ntitle==>",title)
        new_df=df.where(df['Contest Title']==title)
        fname_list[1]=fname_list[1][:4]+'_'+title+fname_list[1][4:]
        new_name=('_'.join(fname_list)).strip()
        if not os.path.exists("./O2/"+outpath[26:]+mid+'/'):
            os.makedirs("./O2/"+outpath[26:]+mid+'/')
        #print("No of rows ==>",new_df.count(),"path==> /"+mid+'/'+new_name)
        new_df.toPandas().to_csv("./O2/"+outpath[26:]+mid+'/'+new_name, header=True, sep='|', index = False)
        fname_list=os.path.basename(file_location).split('_')
        
        
##Get all the paths
paths_dict={}
for (dirpath, dirnames, filenames) in walk("./Output/Alabama2016-2018"):
    for d in dirnames:
        paths_dict[d]={"General":glob.glob(dirpath+"/"+d+"/General/*.csv"),"Primary":glob.glob(dirpath+"/"+d+"/Primary/*.csv"),"outG":dirpath+"/"+d+"/General/","outP":dirpath+"/"+d+"/Primary/"}

    break        

#Run it for all files
for i in ['2018','2016']:
    print(i)
    for j in paths_dict[i]["General"]:
        Etl_job2(j,paths_dict[i]["outG"])
        
    for j in paths_dict[i]["Primary"]:
        Etl_job2(j,paths_dict[i]["outP"])
        
sc.stop() 
print("END")