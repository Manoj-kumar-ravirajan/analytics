--Problem Statements 01
--Q1 Display all information in the table EMP and DEPT?
SELECT
	*
FROM
	employees e
SELECT
	*
FROM
	departments d

--Q2 Display only the hire date and employee name for each employee?
SELECT
	hire_date,
	first_name
FROM
	employees e

--Q3 Display the ename(first_name _ lastName) concatenated with the job id , separated by a command and space and name the column as employee and title.
SELECT
	first_name || ' ' || last_name || ' , ' || job_id as employee
FROM
	employees e

--Q4 Display the hire date, name and department number for all clerks
SELECT
	hire_date,
	first_name || ' ' || last_name as name,
	department_id
FROM
	employees e
WHERE
	job_id in (
	SELECT
		job_id
	FROM
		jobs
	WHERE
		job_title LIKE '%Clerk%')

--Q5 Display the names and salaries of all employees with a salary greater than 3000
SELECT
	first_name || ' ' || last_name as name,
	salary
FROM
	employees e
WHERE
	salary > 3000

--Q6 Disply the last_name, job, and salary for all employes whose job is “sales representative’ or stcoks_clerk and where salary is not equal to $2500, 3500,5500 
SELECT
	last_name,
	job_title job,
	salary
FROM
	employees e
JOIN jobs j on
	e.job_id = j.job_id
WHERE 
	salary NOT IN (2500, 3500, 5500)
	AND job_title IN ('Stock Clerk', 'Sales Representative')
	
--Problem Statements 02	
--Q1 Display the maximum, minimum, and average salary of employee?
SELECT
	max(salary) maximum,
	min(salary) mininum,
	avg(salary) average
FROM
	employees e

--Q2 Display the department id and number of employees in each department>
SELECT
	department_id,
	COUNT(DISTINCT employee_id)  number_of_employees
FROM 
	employees e 
GROUP BY department_id

--Q3 Display the department id and total salary of employee in each department
SELECT
	department_id,
	SUM(salary)  total_salary
FROM 
	employees e 
GROUP BY department_id

--Q4 Display the sum of salaries of the employees working under each manager
SELECT 
	e.manager_id,
	e2.first_name || ' ' || e2.last_name as name,
	SUM(e.salary) sum_of_salaries
FROM 
	employees e
JOIN 
	employees e2 ON
	e.manager_id = e2.employee_id
GROUP BY
	e.manager_id

--Q5 Select the manager name , the count of employees working under and the department_id of the manager?
SELECT 
	e.manager_id,
	e2.first_name || ' ' || e2.last_name as name,
	d.department_name,
	COUNT(DISTINCT e.employee_id) count_of_employees
FROM 
	employees e
JOIN 
	employees e2 ON
	e.manager_id = e2.employee_id
JOIN departments d ON
	e.department_id = d.department_id 
GROUP BY
	e.manager_id, department_name
 
--Q6 Select the maximum salary of each department with the department_id, department_name?
SELECT 
	e.department_id,
	d.department_name,
	MAX(e.salary) maximum_salary
FROM 
	employees e 
JOIN 
	departments d ON
	e.department_id = d.department_id 
GROUP BY e.department_id, d.department_name 

--Problem Statements 03
--Q1 Write a query to display the last_name and if half of the salary is greater than 10,000 then increase the salary by 10% else by 12.5 along with the bonus amount of 5000 

SELECT
	last_name,
	salary,
	CASE 
		WHEN (salary/2) > 10000 THEN salary+salary*(10/100) + 5000
		ELSE salary+salary*(12.5/100) + 5000
	END bonus_amount 
FROM employees e 
	
--Problem Statements 04
--Q1 Write a query to display the last_name, department id, department_name for all employees.

SELECT 
	e.last_name,
	e.department_id,
	d.department_name
FROM 
	employees e 
JOIN 
	departments d ON
	e.department_id = d.department_id 
	
--Q2 Write a qwery that display the name, job, department_naem, salary, garade for all employees (Salary > = 50000 A, Salary >=30000 B, C)
SELECT 
	first_name || ' ' || last_name as name,
	job_title,
	department_name,
	CASE
		WHEN salary >= 20000 THEN 'A'
		WHEN salary >= 10000 THEN 'B'
		Else 'C'
	END salary
	
FROM 
	employees e 
JOIN 
	jobs j ON
	e.job_id = j.job_id 
JOIN departments d ON
	d.department_id = e.department_id

--Problem Statements 05
--Q1 Write a query to display the first_name and hire_date of employee in the same department as SALES.

SELECT
	first_name,
	hire_date
FROM
	employees e
WHERE
	department_id IN (
	SELECT
		department_id
	FROM
		departments d
	WHERE
		department_name like '%Sales%')
	
	