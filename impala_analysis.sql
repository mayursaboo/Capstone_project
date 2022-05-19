
select s.emp_no, e.last_name, e.first_name, e.sex, s.salary from Employees as e inner join Salaries as s on s.emp_no = e.emp_no order by s.emp_no;

select emp_no, last_name, first_name, hire_date from Employees where year(to_date(from_unixtime(unix_timestamp(hire_date, 'dd/MM/yyyy')))) = 1986;

select  Departmentt_Managers.dept_no, Department.dept_name, Department_Managers.emp_no, Employees.last_name, Employees.first_name from Department_Managers inner join Department on Department_Managers.dept_no= Department.dept_no inner join Employees on Department_Managers.emp_no = Employees.emp_no order by Department_Managers.dept_no;

select e.emp_no, e.last_name, e.first_name,d.dept_name from Employees as e left join Depart_Employee as de on e.emp_no = de.emp_no inner join Department as d on de.dept_no = d.dept_no order by e.emp_no;

select e.last_name, e.first_name from Employees as e where e.first_name = 'Hercules' and e.last_name like 'B%' order by e.last_name;

select Employees.emp_no, Employees.last_name, Employees.first_name, Department.dept_name from  Department inner join  Depart_Employee on Department.dept_no = Depart_Employee.dept_no inner join Employees on Depart_Employee.emp_no = Employees.emp_no where dept_name = '"Sales"';

select Employees.emp_no, Employees.last_name, Employees.first_name, Department.dept_name from  Department inner join  Depart_Employee on Department.dept_no = Depart_Employee.dept_no inner join Employees on Depart_Employee.emp_no = Employees.emp_no where Department.dept_name like '%Sales%' or Department.dept_name like '%development%';

select last_name,count(last_name) as frequency from Employees group by last_name order by frequency desc;

select t1.emp_no, t2.emp_no, t1.first_name, t1.last_name,t1.sex,t2.salary from Employees t1 inner join Salaries t2 on t1.emp_no = t2.emp_no;

select t1.title, avg(t3.salary) as avg_salary from Titles t1 inner join Employees t2 on t1.title_id = t2.emp_title_id inner join Salaries t3 on t2. 'dd/MM/yyyy')))) as ended, year(cast(to_date(from_unixtime(unix_timestamp(last_date, 'dd/MM/yyyy')))))-year(cast(to_date(from_unixtime(unix_timestamp(hire_date, 'dd/MM/yyyy'))))) from Employees;



