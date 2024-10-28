# C++ SQL Database

## Example Output
### Create Table

#### Syntax
`CREATE TABLE <table_name>(<col_1_name> <col_1_type>, <col_2_name> <col_2_type>, ...)`
#### Input Query
`./db "create table course(id int, name varchar(10), total_credits int)"`
#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "create table course(id int, name varchar(10), total_credits int)"

CREATE TABLE statement

```

***

### List Tables

#### Syntax
`LIST TABLE`

#### Input Query
`./db "list table"`
#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "list table"

LIST TABLE statement

Table List
*****************
student
employee
department
course
****** End ******
```

***

### List Table Schema

#### Syntax
`LIST SCHEMA FOR <table_name>`

#### Input Query
`./db "list schema for student"`
#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "list schema for student"

LIST SCHEMA statement
Table PD size            (tpd_size)    = 180
Table Name               (table_name)  = student
Number of Columns        (num_columns) = 4
Column Descriptor Offset (cd_offset)   = 36
Table PD Flags           (tpd_flags)   = 0

Column Name   (col_name) = id
Column Id     (col_id)   = 0
Column Type   (col_type) = 10
Column Length (col_len)  = 4
Not Null flag (not_null) = 0

Column Name   (col_name) = first
Column Id     (col_id)   = 1
Column Type   (col_type) = 12
Column Length (col_len)  = 10
Not Null flag (not_null) = 0

Column Name   (col_name) = last
Column Id     (col_id)   = 2
Column Type   (col_type) = 12
Column Length (col_len)  = 10
Not Null flag (not_null) = 0

Column Name   (col_name) = age
Column Id     (col_id)   = 3
Column Type   (col_type) = 10
Column Length (col_len)  = 4
Not Null flag (not_null) = 0
```

***

### Drop Table

#### Syntax
`DROP TABLE <table_name>`

#### Input Query
`./db "drop table course"`
#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "drop table course"

DROP TABLE statement
```

***

### Insert Query

#### Syntax
`INSERT INTO <table_name> VALUES ( <col_1_val>, <col_2_val>, ...)`

#### Input Query 1
`./db "insert into employee values (1001, 'Aditya', 'Dawadikar', 2001)"`

#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "insert into employee values (1001, 'Aditya', 'Dawadikar', 2001)"

INSERT ROW statement
emp_id: (int) 1001 
f_name:  (char*) Aditya 
l_name:  (char*) Dawadikar 
dept: (int) 2001 
Inserted 1 Row.

```

#### Input Query 2
`./db "insert into department values (NULL, 'Nvidia')"`
#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "insert into department values (NULL, 'Nvidia')"

INSERT ROW statement
Error: Column 'dept_id' cannot be NULL.
```

#### Input Query 3
`./db "insert into department values ('2004', NULL)"`
#### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "insert into department values ('2004', NULL)"

INSERT ROW statement
dept_id: (char*) 2004
(NULL char*) 
Inserted 1 Row.
```


***

### Select Query

#### Select All
##### Syntax
`SELECT * FROM <table_name>`

##### Query
`./db "select * from employee"`
##### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "select * from employee"

SELECT statement
--------------------------------------------------------------------------------------------
| emp_id               | f_name               | l_name               | dept                 | 
--------------------------------------------------------------------------------------------
| 1001                 | Aditya               | Dawadikar            | 2001                 | 
| 1002                 | Rudraksh             | Naik                 | 2001                 | 
| 1003                 | Shubhankar           | Munshi               | 2002                 | 
| 1004                 | Mandar               | Gondane              | 2002                 | 
--------------------------------------------------------------------------------------------
```

***

#### Projection

##### Syntax
`SELECT <col_1_name>, <col_2_name>, ... FROM <table_name>`
##### Query
`./db "select emp_id, f_name, l_name, dept from employee"`
##### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "select emp_id, f_name, l_name, dept from employee"

SELECT statement
--------------------------------------------------------------------------------------------
| emp_id               | f_name               | l_name               | dept                 | 
--------------------------------------------------------------------------------------------
| 1001                 | Aditya               | Dawadikar            | 2001                 | 
| 1002                 | Rudraksh             | Naik                 | 2001                 | 
| 1003                 | Shubhankar           | Munshi               | 2002                 | 
| 1004                 | Mandar               | Gondane              | 2002                 | 
--------------------------------------------------------------------------------------------
```

***

#### Natural Join

##### Syntax
```
SELECT
<table_1_col_1>, <table_1_col_2>,...,<table_2_col_1>,<table_2_col_2>,...
FROM
<table_1> NATURAL JOIN <table_2>
ON
<table_1_join_col> = <table_2_join_col>
```

##### Query
`./db "select emp_id, f_name, l_name, dept_name from employee natural join department on dept = dept_id"`
##### Output
```
root@0ab80bc98dab:/usr/src/cpp_sql_database# ./db "select emp_id, f_name, l_name, dept_name from employee natural join department on dept = dept_id"

SELECT statement
--------------------------------------------------------------------------------------------
| emp_id               | f_name               | l_name               | dept_name            | 
--------------------------------------------------------------------------------------------
| 1001                 | Aditya               | Dawadikar            | AWS                  | 
| 1002                 | Rudraksh             | Naik                 | AWS                  | 
| 1003                 | Shubhankar           | Munshi               | GCP                  | 
| 1004                 | Mandar               | Gondane              | GCP                  | 
--------------------------------------------------------------------------------------------
```

***

## TODOs and Limitations
1. NATURAL JOIN does not automatically detect columns with same names.
2. NATURAL JOIN does not support `table.column` notation for columns list. Eg: `SELECT employee.f_name, department.dept_id FROM employee natural join department on employee.dept = department.dept_id` is `INVALID`. Thus column names must be unique across the database, but no check is added to ensure uniqueness across database.
