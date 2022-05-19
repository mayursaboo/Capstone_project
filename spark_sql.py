#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = (SparkSession.builder.appName("project").config("hive.metastore.uris","thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083").enableHiveSupport().getOrCreate())


# In[2]:


spark


# In[3]:


department = spark.sql("select * from anabig114231.deprtment")


# In[4]:


employee_df = spark.sql("select * from anabig114231.employeees")


# In[5]:


dept_manager = spark.sql("select * from dept_manager")


# In[6]:


dept_employee = spark.sql("select * from dept_employee")


# In[7]:


salaries = spark.sql("select * from anabig114231.salaries")


# In[ ]:


## 1. A list showing employee number, last name, first name, sex, and salary for each employee1. A list showing first name, last 
##    name, and hire date for employees who were hired in 1986.


# In[10]:


tble_1 = spark.sql(""" select s.emp_no, e.first_name, e.last_name, e.sex 
from anabig114231.employeees as e inner join anabig114231.salaries as s on s.emp_no = e.emp_no 
order by s.emp_no""").show()


# In[ ]:


## 2. A list showing first name, last name, and hire date for employees who were hired in 1986.


# In[30]:


tble_2 = spark.sql(""" select emp_no, first_name, last_name, hire_date from anabig114231.employeees 
where year(to_date(from_unixtime(unix_timestamp(hire_date, 'dd/MM/yyyy')))) = 1986""").show()


# In[16]:


## 3. A list showing the manager of each department with the following information: department number, department name, 
##    the manager's employee number, last name, first name


# In[14]:


tble_3 = spark.sql(""" select anabig114231.dept_manager.dept_no, anabig114231.deprtment.dept_name, 
anabig114231.dept_manager.emp_no,anabig114231.employeees.first_name, anabig114231.employeees.last_name 
from anabig114231.dept_manager inner join anabig114231.deprtment on anabig114231.dept_manager.dept_no = anabig114231.deprtment.dept_no 
inner join anabig114231.employeees on anabig114231.dept_manager.emp_no = anabig114231.employeees.emp_no 
order by anabig114231.dept_manager.dept_no""").show()


# In[17]:


## 4. A list showing the department of each employee with the following information: employee number, last name, first 
##    name, and department name.


# In[20]:


tble_4 = spark.sql(""" select anabig114231.employeees.emp_no, anabig114231.employeees.first_name ,anabig114231.employeees.last_name, 
anabig114231.deprtment.dept_name from anabig114231.employeees left join anabig114231.dept_employee on 
anabig114231.employeees.emp_no = anabig114231.dept_employee.emp_no 
inner join anabig114231.deprtment on anabig114231.dept_employee.dept_no = anabig114231.deprtment.dept_no 
order by anabig114231.employeees.emp_no""").show()


# In[21]:


## 5 .A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.


# In[42]:


tble_5 = spark.sql(""" select first_name ,last_name from anabig114231.employeees 
where first_name = 'Hercules'and last_name like 'B%' 
order by last_name""").show()


# In[28]:


## 6 A list showing all employees in the Sales department, including their employee number, last name, first name, and 
 ##  department name.


# In[ ]:


tble_12= spark.sql("""select employeees.emp_no, employeees.last_name, employeees.first_name, deprtment.dept_name 
from  deprtment
inner join  dept_employee on deprtment.dept_no = dept_employee.dept_no
inner join employeees on dept_employee.emp_no = employeees.emp_no
where dept_name = '"Sales"'""").show()


# In[29]:


tble_6 = spark.sql(""" select anabig114231.employeees.emp_no, anabig114231.employeees.first_name ,anabig114231.employeees.last_name,
anabig114231.deprtment.dept_name from anabig114231.deprtment inner join anabig114231.dept_employee 
on anabig114231.deprtment.dept_no = anabig114231.dept_employee.dept_no
inner join anabig114231.employeees on anabig114231.dept_employee.emp_no = anabig114231.employeees.emp_no  
where anabig114231.deprtment.dept_name = '"Sales"'""").show()


# In[31]:


## 7. A list showing all employees in the Sales and Development departments, including their employee number, last name, 
##  first name, and department name.


# In[34]:


tble_7 = spark.sql(""" select anabig114231.employeees.emp_no, anabig114231.employeees.first_name ,anabig114231.employeees.last_name,
anabig114231.deprtment.dept_name from anabig114231.deprtment inner join anabig114231.dept_employee 
on anabig114231.deprtment.dept_no = anabig114231.dept_employee.dept_no
inner join anabig114231.employeees on anabig114231.dept_employee.emp_no = anabig114231.employeees.emp_no  
where anabig114231.deprtment.dept_name like '%Sales%' or anabig114231.deprtment.dept_name like '%development%' """).show()


# In[35]:


## 8. A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each 
##    last name


# In[ ]:


tble_13= spark.sql("""select last_name,count(last_name) as frequency 
from employeees 
group by last_name
order by frequency desc""");


# In[39]:


tble_8 = spark.sql(""" select last_name, count(last_name) as frequency from anabig114231.employeees 
group by last_name
order by frequency desc""").show()


# In[37]:


## 9 Histogram to show the salary distribution among the employees


# In[26]:


import pandas as pd
import matplotlib.pyplot as plt


# In[32]:


employee_df1 = employee_df.toPandas()


# In[21]:


salaries_df = spark.sql("select * from anabig114231.salaries")


# In[22]:


salaries_df1 = salaries_df.toPandas()


# In[25]:


emp_sal = pd.merge(employee_df1, salaries_df1, how="inner", on="emp_no")


# In[27]:


emp_sal.hist(column='salary')


# In[ ]:


## 10 Bar graph to show the Average salary per title (designation)


# In[29]:


title_df = spark.sql("select * from anabig114231.tittles")


# In[30]:


title_df1 = title_df.toPandas()


# In[40]:


employee_df2= employee_df1.rename(columns={'emp_title_id':'title_id'})


# In[48]:


emp_sal1 = pd.merge(employee_df2, salaries_df1, how="left", on="emp_no")


# In[49]:


emp_sal1.columns


# In[50]:


emp_title = pd.merge(emp_sal1, title_df1, how="left", on="title_id")


# In[59]:


emp_title.columns


# In[64]:


employee_title = emp_title.groupby(['title'])['salary'].mean()
employee_title


# In[65]:


employee_title.plot(kind = 'bar',figsize = (8,8))


# In[ ]:


## 11. What is the average salary according to Gender 


# In[73]:


gender_salary = emp_title.groupby(['sex'])['salary'].mean()
gender_salary


# In[75]:


gender_salary.plot(kind = 'bar',figsize = (5,5))


# In[ ]:


## 12. Number of projects according to title


# In[80]:


title_projects = emp_title.groupby(['title'])['no_of_projects'].count()
title_projects


# In[81]:


title_projects.plot(kind = 'bar',figsize = (5,5))


# In[ ]:


## 13. Number of projects gender wise


# In[82]:


project_gender = emp_title.groupby(['sex'])['no_of_projects'].count()
project_gender


# In[83]:


project_gender.plot(kind = 'bar',figsize = (5,5))


# In[ ]:




