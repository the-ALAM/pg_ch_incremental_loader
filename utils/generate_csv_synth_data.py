import csv
import random
import time
from datetime import datetime, timedelta

# Data pools for synthetic generation
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
          'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 
          'San Francisco', 'Columbus', 'Fort Worth', 'Charlotte', 'Seattle', 'Denver', 
          'Boston', 'Nashville']

countries = ['USA', 'Canada', 'Mexico', 'UK', 'Germany', 'France', 'Spain', 'Italy']

first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'James', 'Jessica', 
               'Robert', 'Amanda', 'William', 'Lisa', 'Richard', 'Jennifer', 'Joseph', 
               'Michelle', 'Thomas', 'Ashley', 'Christopher', 'Kimberly']

last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
              'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson', 
              'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee']

branch_names = ['Main Branch', 'Downtown Branch', 'Central Branch', 'North Branch', 
                'South Branch', 'East Branch', 'West Branch', 'Park Branch', 
                'City Branch', 'Plaza Branch']

store_names = ['Electronics Store', 'Grocery Store', 'Fashion Store', 'Book Store', 
               'Sports Store', 'Home Store', 'Toy Store', 'Beauty Store', 
               'Pet Store', 'Garden Store']

streets = ['Main St', 'Oak Ave', 'Park Blvd', 'First St', 'Second Ave', 'Elm Street', 
           'Maple Drive', 'Cedar Lane', 'Pine Road', 'Washington Blvd']

# Read existing IDs
with open('resources/dim_tbl-brch.csv', 'r') as f:
    branch_ids = [line.strip().strip("'") for line in f.readlines()[1:]]

with open('resources/dim_tbl-cash.csv', 'r') as f:
    cashier_ids = [line.strip().strip("'") for line in f.readlines()[1:]]

with open('resources/dim_tbl-cust.csv', 'r') as f:
    customer_ids = [line.strip().strip("'") for line in f.readlines()[1:]]

with open('resources/dim_tbl-store.csv', 'r') as f:
    store_ids = [line.strip().strip("'") for line in f.readlines()[1:]]

# Base timestamp (1 year ago)
base_time = int(time.time()) - 86400 * 365

# Generate branches CSV
with open('resources/dim_tbl-brch.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'address', 'city', 'country', 'created_at', 'updated_at'])
    for i, bid in enumerate(branch_ids):
        writer.writerow([
            bid,
            f'{random.choice(branch_names)} #{i+1}',
            f'{random.randint(100, 9999)} {random.choice(streets)}',
            random.choice(cities),
            random.choice(countries),
            base_time + random.randint(0, 86400*365),
            base_time + random.randint(0, 86400*365)
        ])

# Generate cashiers CSV
with open('resources/dim_tbl-cash.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'branch_id', 'created_at', 'updated_at'])
    for cid in cashier_ids:
        writer.writerow([
            cid,
            f'{random.choice(first_names)} {random.choice(last_names)}',
            random.choice(branch_ids),
            base_time + random.randint(0, 86400*365),
            base_time + random.randint(0, 86400*365)
        ])

# Generate app_users CSV
with open('resources/dim_tbl-cust.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'phone_number', 'name', 'created_at', 'updated_at'])
    for uid in customer_ids:
        phone = f'+1{random.randint(2000000000, 9999999999)}' if random.random() > 0.2 else ''
        name = f'{random.choice(first_names)} {random.choice(last_names)}' if random.random() > 0.1 else ''
        writer.writerow([
            uid,
            phone,
            name,
            base_time + random.randint(0, 86400*365),
            base_time + random.randint(0, 86400*365)
        ])

# Generate stores CSV
with open('resources/dim_tbl-store.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'branch_id', 'created_at', 'updated_at'])
    for i, sid in enumerate(store_ids):
        writer.writerow([
            sid,
            f'{random.choice(store_names)} #{i+1}',
            random.choice(branch_ids),
            base_time + random.randint(0, 86400*365),
            base_time + random.randint(0, 86400*365)
        ])

print("CSV files generated successfully!")
