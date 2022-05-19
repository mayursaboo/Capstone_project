#!/usr/bin/env python
# coding: utf-8

# In[54]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = (SparkSession.builder.appName("project").config("hive.metastore.uris","thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083").enableHiveSupport().getOrCreate())


# In[55]:


spark


# In[56]:


department_df = spark.sql("select * from anabig114231.deprtment")


# In[57]:


employee_df = spark.sql("select * from anabig114231.employeees")


# In[58]:


dept_manager_df = spark.sql("select * from dept_manager")


# In[59]:


dept_employee_df = spark.sql("select * from dept_employee")


# In[60]:


salaries_df = spark.sql("select * from anabig114231.salaries")


# In[61]:


title_df = spark.sql("select * from anabig114231.tittles")


# In[62]:


df = employee_df.join(dept_employee_df,on = 'emp_no', how = 'left')         .join(department_df, on='dept_no',how = 'left')         .join(salaries_df, on ='emp_no', how = 'left')         .join(title_df, employee_df.emp_title_id == title_df.title_id, 'left')


# In[49]:


df.show()


# In[46]:


#Any null(missings) values
from pyspark.sql.functions import isnull
df_clean = df.na.drop( how = 'any' )


# In[47]:


df_clean.count() == df.count()


# In[48]:


df.printSchema()


# In[63]:


import matplotlib.pyplot as plt
import seaborn as sn
get_ipython().run_line_magic('matplotlib', 'inline')


# In[64]:


import pandas as pd


# In[65]:


df1 = df.toPandas()


# In[ ]:


title_salary = df1.groupby('title')['salary'].sum()

plt.pie(title_salary,labels =['staff','senior_staff','asst_engineer','senior_engineer','engineer','Manager','technique_leader'],autopct="%1.0f%%", colors = ['r','g','b'])
plt.title('salary per title')

plt.show()


# In[38]:


salary_gender_title = df1.groupby(['sex','title'])['no_of_projects'].sum()

salary_gender_title.plot(kind = 'bar',figsize=(8,8), width = 0.8)
plt.ylabel('No_of_Projects')


plt.show()


# In[83]:


gender_salary = df1.groupby('sex')['salary'].sum()

plt.pie(gender_salary,labels= ['F','M'],autopct="%1.0f%%", colors = ['r','g'])


plt.show()


# In[84]:


dept_salary = df1.groupby(['dept_name'])['salary'].sum()

dept_salary.plot(kind = 'bar',figsize=(8,8), width = 0.8)
plt.ylabel('Salary')
plt.title('Salary w r t dept')

plt.show()


# In[37]:


dept_project = df1.groupby('dept_name')['no_of_projects'].sum()

plt.pie(dept_project,labels =['Marketing','Finance','Human_resource','Customer_service','development','Quality_Management','Sales','Research','Production'],autopct="%1.0f%%", colors = ['r','g','b','y'])


plt.show()


# In[66]:


#Columns that will be used as features and their types
continuous_features = ['salary','no_of_projects']
                    
categorical_features = ['dept_name', 'dept_no','title']


# In[96]:


#Encoding all categorical features
from pyspark.ml.feature import OneHotEncoderEstimator, OneHotEncoder, StringIndexer, VectorAssembler, PolynomialExpansion, VectorIndexer


# In[68]:


# create object of StringIndexer class and specify input and output column
SI_dept_name = StringIndexer(inputCol='dept_name',outputCol='dept_name_Idx5')
SI_dept_no = StringIndexer(inputCol='dept_no',outputCol='dept_no_Idx5')
SI_title = StringIndexer(inputCol='title',outputCol='title_Idx5')



# transform the data
df = SI_dept_name.fit(df).transform(df)
df = SI_dept_no.fit(df).transform(df)
df = SI_title.fit(df).transform(df)



# view the transformed data
df.select('dept_name', 'dept_name_Idx5', 'dept_no', 'dept_no_Idx5', 'title','title_Idx5').show(5)


# In[69]:


# create object and specify input and output column
OHE = OneHotEncoderEstimator(inputCols=['dept_name_Idx5', 'dept_no_Idx5','title_Idx5'],outputCols=['dept_name_vec5', 'dept_no_vec5','title_vec5'])

# transform the data
df = OHE.fit(df).transform(df)

# view and transform the data
df.select('dept_name', 'dept_name_Idx5', 'dept_name_vec5', 'dept_no', 'dept_no_Idx5', 'dept_no_vec5', 'title','title_Idx5','title_vec5').show(10)


# In[70]:


featureCols = continuous_features + ['dept_name_vec5',   'dept_no_vec5','title_vec5'] 


# In[71]:


assembler = VectorAssembler( inputCols = featureCols, outputCol = "features")


# In[72]:


train_df1 = assembler.transform( df )


# In[73]:


train_df1 = train_df1.withColumn('label', train_df1['left1'].cast('integer'))


# In[74]:


#Split the dataset
train_df, test_df = train_df1.randomSplit( [0.7, 0.3], seed = 42 )


# In[75]:


from pyspark.ml.classification import LogisticRegression


# In[76]:


#linreg = LinearRegression(maxIter=500, regParam=0.0)
linreg = LogisticRegression()


# In[77]:


lm = linreg.fit( train_df )


# In[78]:


#Make predictions on train data and evaluate
y_pred_train = lm.transform(train_df)


# In[79]:


y_pred_test = lm.transform( test_df )


# In[80]:


y_pred_test.select( 'features',  'label', 'prediction', 'left1' ).show( 5 )


# In[ ]:


## Random Forest Classifier


# In[81]:


from pyspark.ml.classification import RandomForestClassifier
RF = RandomForestClassifier( featuresCol='features', labelCol='label')
RF_model = RF.fit(train_df)


# In[82]:


predictions = RF_model.transform(test_df)


# In[83]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[84]:


evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(predictions)
accuracy


# In[ ]:


## Gradient Boost Classifier


# In[85]:


from pyspark.ml.classification import GBTClassifier
gbt = GBTClassifier(maxIter = 10, featuresCol='features', labelCol='label')
gbt_model = gbt.fit(train_df)


# In[86]:


predictions = gbt_model.transform(test_df)


# In[87]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[88]:


evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(predictions)
accuracy


# In[ ]:


## Create Pipeline


# In[89]:


continuous_features


# In[90]:


categorical_features


# In[93]:


#create indexers for the categorical features
indexers=[StringIndexer(inputCol=c,outputCol="{}_idx".format(c)) for c in categorical_features]


# In[97]:


##encode the categorical features

encoders=[OneHotEncoder(inputCol=idx.getOutputCol(),outputCol="{0}_enc".format(idx.getOutputCol())) for idx in indexers]


# In[100]:


##create vectors for all the features categorical and continious

assembler=VectorAssembler(inputCols=[enc.getOutputCol() for enc in encoders]+continuous_features,outputCol="features")


# In[102]:


#initiate the linear model

lrModel=LogisticRegression(maxIter=10)


# In[103]:


##create the pipeline with all the above stages

pipeline=Pipeline(stages=["indexers","encoders","assesmbler","lrModel"])


# In[ ]:




