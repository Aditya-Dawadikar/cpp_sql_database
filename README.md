## Supported Queries and Commands

### **1. Create Tables**
Create tables with specified schema:
```bash
./db "create table employee (emp_id int NOT NULL, f_name char(20), l_name char(20), dept char(20))"
./db "create table department (dept_id char(20) NOT NULL, dept_name char(20))"
```

---

### **2. Insert Data**
Insert records into tables:
```bash
./db "insert into employee values (1001, 'Aditya', 'Dawadikar', '2001')"
./db "insert into employee values (1002, 'Rudraksh', 'Naik', '2001')"
./db "insert into employee values (1003, 'Shubhankar', 'Munshi', '2002')"
./db "insert into employee values (1004, 'Mandar', 'Gondane', '2002')"

./db "insert into department values ('2001', 'AWS')"
./db "insert into department values ('2002', 'GCP')"
```

---

### **3. Select Queries**

#### **Basic Select**
Retrieve all or specific columns from a table:
```bash
./db "select * from employee"
./db "select * from department"
./db "select emp_id, f_name, l_name, dept from employee"
./db "select dept_id, dept_name from department"
```

#### **Natural Join**
Perform natural joins between tables:
```bash
./db "select emp_id, f_name, l_name, dept_name from employee natural join department on dept = dept_id"
./db "select emp_id, f_name, l_name, dept_name from employee natural join department on dept = dept_id where dept='2001'"
./db "select emp_id, f_name, l_name, dept_name from employee natural join department on dept = dept_id where dept='2002' order by emp_id"
./db "select * from employee natural join department on dept = dept_id"
./db "select * from employee natural join department on dept = dept_id where dept_name='GCP'"
./db "select * from employee natural join department on dept = dept_id where dept_id='2002'"
./db "select * from employee natural join department on dept = dept_id order by emp_id"
./db "select * from employee natural join department on dept = dept_id order by emp_id where dept_name='GCP'"
```

#### **Conditional Select**
Filter rows using WHERE conditions:
```bash
./db "select * from employee where emp_id=1001 or l_name='Munshi'"
```

---

### **4. Aggregate Functions**
Perform aggregate operations like `COUNT`, `SUM`, and `AVG`:
```bash
./db "select count(emp_id) from employee"
./db "select sum(emp_id) from employee"
./db "select avg(emp_id) from employee"
./db "select count(emp_id) from employee where dept='2002'"
./db "select count(emp_id) from employee natural join department on dept = dept_id"
./db "select count(emp_id) from employee natural join department on dept = dept_id where dept_name='GCP'"
```

---

### **5. Modify Data**

#### **Delete Records**
Delete specific rows from a table:
```bash
./db "delete from employee where emp_id=1001"
```

#### **Update Records**
Update specific rows in a table:
```bash
./db "update employee set emp_id=3001 where emp_id=1001"
```

---

### **6. Utility Commands**

#### **Drop Table**
Remove a table from the database:
```bash
./db "drop table course"
```

#### **List Tables**
List all tables in the database:
```bash
./db "list table"
```

#### **List Schema**
Retrieve schema details for a specific table:
```bash
./db "list schema for student"
```

--- 

### Notes:
1. **Order By Support:** Sorting is supported in select queries (both basic and join) using the `ORDER BY` clause.
2. **Aggregate Functions:** Supported aggregate functions include `COUNT`, `SUM`, and `AVG`.
3. **Joins:** Natural joins are supported using the `NATURAL JOIN` clause with the `ON` condition.
4. **Case Sensitivity:** Aggregate functions and keywords are case-insensitive.
