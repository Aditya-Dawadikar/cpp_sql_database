# A Simple SQL Engine built with C++ 🚀

### Introduction
This project is a lightweight, terminal-based relational database management system (RDBMS) implemented in C++. Designed for simplicity and extensibility, the system supports essential database operations such as creating tables, inserting, updating, and deleting records, and executing queries with filtering, aggregation, and sorting. It includes support for natural joins, aggregate functions like `COUNT`, `SUM`, and `AVG`, and conditional queries with logical operators like `AND` and `OR`. The project demonstrates foundational database functionalities, providing a simple yet powerful tool for managing and querying relational data directly from the command line. This RDBMS is perfect for educational purposes, experimentation, and exploring the inner workings of database systems.

### Data Storage and Manipulation

The database stores its data in binary files, where each table is represented by a dedicated file. Each record in the file includes a "delete flag" to mark rows as logically deleted without physically removing them, ensuring efficient updates and deletions. 

**Insertions** append new records to the end of the file, maintaining a compact and ordered structure.

**Updates** modify the necessary fields in the binary file by seeking to the appropriate record and overwriting only the modified parts.

<mark>**Deletions** mark records with the delete flag, allowing them to be ignored during query execution while keeping the storage layout intact. This approach minimizes I/O operations and maintains file integrity, making operations faster and more efficient.<mark>

### Handling Inserts with Prioritization for Deleted Rows

The database employs an efficient mechanism for managing inserts by reusing space occupied by previously deleted rows. Each record in the binary file includes a "delete flag," which indicates whether the record is active or logically deleted. When a new **insert** operation is performed, the database scans the file for rows marked as deleted and reuses these slots for the new record. This approach optimizes storage by reducing fragmentation and avoiding unnecessary growth of the binary file.

If no deleted rows are found, the new record is appended to the end of the file. By prioritizing previously deleted rows, the database ensures efficient use of disk space, maintains a compact file structure, and improves performance during sequential file scans. This design helps balance resource utilization while keeping the operations fast and reliable.

### Database File Structure 📁

The database system uses two primary file types for storage:

#### 1. **Schema File (`dbfile.bin`)**
- The `dbfile.bin` file serves as the master schema repository for the database. It contains metadata about all registered tables, including:
  - Table names.
  - Column definitions (e.g., column names, data types, sizes, constraints).
  - The number of columns in each table.
- This file is critical for query execution as it allows the system to validate table existence and column definitions during query parsing and execution.
- When a new table is created, its schema is appended to `dbfile.bin`. Similarly, when a table is dropped, its schema is removed from this file.

#### 2. **Table Data Files (`<tablename>.tab`)**
- Each table in the database has a corresponding `.tab` file that contains the actual data stored in a binary format.
- The structure of a `.tab` file includes:
  - A **table header**, which stores metadata such as the record size, number of records, and a pointer to the start of the data section.
  - **Data rows**, which contain the actual values for each column in a row. Rows are stored in sequential order.
  - A **deleted flag** for each row, marking whether the row is active or deleted. Deleted rows are reused for new inserts to optimize storage.
- The `.tab` file is accessed and updated directly during insert, update, delete, and query operations.

#### Interaction Between the Files
- During query execution:
  - The system first reads `dbfile.bin` to fetch the table schema.
  - It then accesses the corresponding `.tab` file to perform operations on the actual data, such as reading rows, inserting new records, or calculating aggregates.

This two-tiered file structure ensures a clear separation of schema and data, making the database more maintainable and efficient. The schema in `dbfile.bin` provides a centralized reference for metadata, while the `.tab` files focus solely on storing and managing table-specific data.

---

### Setup Steps 🛠️

Follow these steps to set up the GCC environment inside a Docker container and compile the project:

#### 1. Install Docker Desktop
- Install Docker Desktop on your system. Choose the appropriate version for your platform:
  - **Mac** (Intel or M1/M2 chips)
  - **Windows**
  - **Linux**

#### 2. Set Environment for M1/M2 Mac Users
- If you are using a Mac with an M1 or M2 chip, execute this command in the terminal before proceeding:
  ```bash
  export DOCKER_DEFAULT_PLATFORM=linux/amd64
  ```

#### 3. Pull the GCC Docker Image
- Pull the official GCC image from Docker Hub:
  ```bash
  docker pull gcc
  ```

#### 4. Create and Start a Docker Container
- Create and run a container named `mygcc` using the GCC image:
  ```bash
  docker run -itd --name mygcc --privileged=true -p 60000:60000 gcc
  ```

#### 5. Access the Container
- Enter the container's shell with root privileges:
  ```bash
  docker exec -ti mygcc bash -c "su"
  ```

#### 6. Sanity Check GCC Installation
- Verify GCC is installed:
  ```bash
  gcc --version
  ```
- Check if GDB (GNU Debugger) is installed:
  ```bash
  gdb --version
  ```
  If it returns an error like `gdb: can't find command`, proceed to install it.

#### 7. Install GDB
- Update the container's package repository:
  ```bash
  apt-get update
  ```
- Install GDB:
  ```bash
  apt-get install gdb
  ```
- Verify GDB installation:
  ```bash
  gdb --version
  ```

#### 8. Transfer Source Files to the Container
- Copy the project files (`db.cpp` and `db.h`) from your host system to the Docker container:
  ```bash
  docker cp db.cpp mygcc:/db.cpp
  docker cp db.h mygcc:/db.h
  ```

#### 9. Compile the Project
- Compile the source code using GCC with debugging enabled:
  ```bash
  gcc -g -o db db.cpp
  ```
  - `-g` enables debugging symbols for use with GDB.
  - `-o db` specifies the output executable name as `db`.

#### 10. Ready to Execute
- Run the database executable:
  ```bash
  ./db "your-query-here"
  ```
  Replace `"your-query-here"` with any supported query syntax. For example:
  ```bash
  ./db "select * from employee"
  ```

If the container mygcc is already created but stopped, start it using:
bash
```bash
docker start "your-container-name-here"
```
You’re now ready to test and debug your database project within the Docker container! 🚀🚀🚀

---

### **Utility Commands**

#### **Create Table**
Add a new table to the database:
```bash
./db "create table department (dept_id char(20) NOT NULL, dept_name char(20))"
```

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

## Demo Queries 📺
#### Let us create a table Employee and insert some data
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/create_01.png)

#### We can print data using various select queries
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/select_01.png)
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/select_02.png)

#### We can also add Natural Joins, Where clause and Order By to our queries
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/select_03.png)
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/select_04.png)

#### We can compute aggregates too!
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/select_06.png)

#### Update rows
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/update_01.png)

#### Delete rows
![img](https://github.com/Aditya-Dawadikar/cpp_sql_database/blob/main/views/delete_01.png)

### Notes:
1. **Order By Support:** Sorting is supported in select queries (both basic and join) using the `ORDER BY` clause.
2. **Aggregate Functions:** Supported aggregate functions include `COUNT`, `SUM`, and `AVG`.
3. **Joins:** Natural joins are supported using the `NATURAL JOIN` clause with the `ON` condition.
