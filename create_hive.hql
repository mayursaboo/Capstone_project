create database analytixlab_db;
use analytixlab_db; 
create external table Employees(emp_no int,emp_title_id varchar(10),birth_date varchar(20),first_name varchar(20),last_name varchar(20),sex varchar(2),hire_date varchar(20),no_of_projects int,Last_performance_rating varchar(2),left_ boolean,last_date varchar(20)) STORED AS PARQUET LOCATION "/user/anabig114231/capstone_project/Employees";
create external table Salaries(emp_no int,salary int) STORED AS PARQUET LOCATION "/user/anabig114231/capstone_project/Salaries";
create external table Titles(title_id varchar(10),title varchar(20)) STORED AS PARQUET LOCATION "/user/anabig114231/capstone_project/Titles";
create external table Department(dept_no varchar(10),dept_name varchar(20)) STORED AS PARQUET LOCATION "/user/anabig114231/capstone_project/Department";
create external table Depart_Employee(emp_no int,dept_no varchar(10)) STORED AS PARQUET LOCATION "/user/anabig114231/capstone_project/Depart_Employee";
create external table Department_Managers(dept_no varchar(10),emp_no int) STORED AS PARQUET LOCATION "/user/anabig114231/capstone_project/Department_Managers";




















