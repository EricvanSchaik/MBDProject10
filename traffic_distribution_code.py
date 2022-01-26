from pyspark.sql.functions import *
# from pyspark.conf import SparkConf
# from pyspark.context import SparkContext
# from IPython.core.display import HTML
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import datetime
from pyspark.sql import SparkSession
import os


# %matplotlib inline
# sc.stop()
# conf = SparkConf()
# conf.set("spark.submit.deployMode", "cluster")
# conf.set("spark.master", "yarn")
# conf.set("spark.dynamicAllocation.maxExecutors", "10")
# sc = SparkContext(conf=conf)
# sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('/user/s1919377/flights/*', header='true')
df = df.withColumn('month',to_timestamp('day', 'yyyy-MM'))
	   
df_baseline = df.where(col('day').contains('2019-01'))
df_progression = df.where(~col('day').contains('2019-01'))
df1 = df_baseline.groupBy('destination').count().sort(col('count').desc()).dropna()
df2 = df_progression.groupBy('destination', 'month').count().sort(col('count').desc()).dropna()

df1_limit = df1.limit(100)

# Take the same airports from the dataframe with everything after 2019-01
df2_limit = df2.join(df1_limit, 'destination', 'leftsemi')


# Create an array of Pandas DataFrames, where each DataFrame contains the one month after 2019-01, 
# with the destination count per airport

df2_rest = df2_limit.sort(col('month').asc())

# Check which DataFrames already exist as csv
filenames = os.listdir('.')
existing_months = []
for f in filenames:
    if f.startswith('distribution-panda-destination-'):
        existing_months.append(datetime.datetime.strptime(f[len('distribution-panda-destination-'):-len('.csv')], '%Y-%m'))
df2_rest = df2_rest.filter(~df2_rest.month.isin(existing_months))

count = df2_rest.count()

while (count > 0):
    month = df2_rest.first()['month']
    df2_month = df2_rest.where(col('month') == month)
    # Put this new DataFrame in the same order as the first DataFrame
    df2_month = df1_limit.withColumnRenamed('count', 'firstCount').join(df2_month, 'destination')
    df2_month = df2_month.sort(col('firstCount').desc()).drop(col('firstCount'))
    df2_month.coalesce(1).write.option('header', 'true').mode('overwrite').csv('distribution-panda-destination-' + month.date().strftime('%Y-%m'))
    
    df2_rest = df2_rest.subtract(df2_month)
    df2_rest.persist()
    count = df2_rest.count()
    
    
# Animate the progression of the air traffic distribution

# Show the distribution of air traffic over the airports on the first day
fig, ax = plt.subplots()

plt.xlabel('Most Popular Airport')
plt.ylabel('Amount of Flights')

ax.set_xlim(0, 100)
ax.set_ylim(0, 25000)

pd1 = df1_limit.toPandas()
ax.plot(pd1.index, pd1['count'])

line, = ax.plot([], [])

def init():
    line.set_data([], [])
    return line,

pds_files = []
filenames = os.listdir('.')
for f in filenames:
    if f.startswith('distribution-panda-destination-'):
        pds_files.append(f)

def animate(i):
    pd_progress = pd.read_csv('./' + pds_files[i])
    
    plt.title(pds_files[i][len('distribution-panda-destination-'):-len('.csv')])
    
    line.set_data(range(len(pd_progress)), pd_progress['count'])
    
    return line,
    
anim = FuncAnimation(fig, animate, init_func = init, frames = len(pds_files), interval = 500)
anim.save('distribution.mp4', writer = 'ffmpeg', fps = 2)

