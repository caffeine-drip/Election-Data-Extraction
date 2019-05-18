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
def Etl_job(inpath,outpath):
    print(inpath,outpath)
    file_location = inpath
    f_name=os.path.basename(file_location)
    fname_list=f_name.split('_')
    df = sqlContext.read.format("csv").option("inferSchema",True).option("header",True).option("delimiter","|").load(file_location)
    
    ##Drop Empty Columns
    drop_list=[]
    for c in df.columns:
        if c.startswith("_c"):
            drop_list.append(c)
    df=df.drop(*drop_list)  
    for n in df.columns:
        df = df.withColumn(n, trim(df[n]))
    
    ##Adding New Columns
    df=df.withColumn("STATE", lit(fname_list[0]))
    df=df.withColumn("Year", lit(fname_list[1].split('-')[0]))
    df=df.withColumn("County", lit(fname_list[2].split('-')[-1]))
   
    ##Check the Values of new Columns
    #print(("STATE", (fname_list[0])),("Year", lit(fname_list[1].split('-')[0])),("County", lit(fname_list[2].split('-')[-1])))
    
    ##Writing DATA using Pyspark
    #df.repartition(1).write.option("header",True).csv("./Output/Spark/"+outpath[2:]+'/'+f_name+".csv", sep='|',mode="overwrite")
    
    ##Writing DATA using Pandas
    if not os.path.exists("./Output/"+outpath[2:]):
        os.makedirs("./Output/"+outpath[2:])
    df.toPandas().to_csv("./Output/"+outpath[2:]+'/'+f_name, header=True,sep='|',index = False)
    
##Get all the paths   
paths_dict={}
for (dirpath, dirnames, filenames) in walk("./Alabama2016-2018"):
    for d in dirnames:
        paths_dict[d]={"General":glob.glob(dirpath+"/"+d+"/General/*.csv"),
                       "Primary":glob.glob(dirpath+"/"+d+"/Primary/*.csv"),
                       "outG":"./Output/"+(dirpath+"/"+d+"/General/")[2:],
                       "outP":"./Output/"+(dirpath+"/"+d+"/Primary/")[2:]
                      }
    break

#Run it for all files
for i in ['2016','2018']:
    
    for j in paths_dict[i]["General"]:
        Etl_job(j,paths_dict[i]["outG"]) 
    for j in paths_dict[i]["Primary"]:
        Etl_job(j,paths_dict[i]["outP"])
    print(i,j,"\n")
        
sc.stop() 
print("END")
        