#!/usr/bin/env python3
"""
Simple Hive Query Tool

Run queries against Hive via Thrift API.
Usage: uv run tools/query-hive.py "SELECT * FROM table LIMIT 5"
Or interactive: uv run tools/query-hive.py

Requires: pyhive in requirements.in, Hive server running on localhost:10000 (port-forwarded).

Example:
CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2))

# Insert data, creates a new block
INSERT INTO students (name, age, gpa) VALUES ('John Doe', 20, 3.5)

# Select data
SELECT * FROM students LIMIT 5

# Compact table
INSERT OVERWRITE TABLE students SELECT * FROM students;

# Compact insert
INSERT OVERWRITE TABLE students SELECT * FROM students UNION ALL VALUES ('wilma flintstone', 34, 2.45), ('betty rubble', 31, 2.78);
"""

import sys
from pyhive import hive
import thrift_sasl

def run_query(query):
    try:
        conn = hive.Connection(host='localhost', port=10000, username='hive')  # Adjust if auth needed
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        print("Results:")
        for row in results:
            print(row)
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

def interactive():
    print("Enter Hive queries (type 'exit' to quit):")
    while True:
        query = input("> ")
        if query.lower() == 'exit':
            break
        if query.strip():
            run_query(query)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        run_query(query)
    else:
        interactive()
