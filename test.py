# Databricks notebook source
# mount bucket
s3_mount_path = "s3a://jake.wong.sg/"
mount_to = "/mnt/jake.wong.sg"

try:
  dbutils.fs.mount(s3_mount_path, mount_to)
  print(s3_mount_path, "s3 mount ok")
except Exception as e:
  print("already mounted")

dbutils.fs.ls("/mnt/jake.wong.sg")

# COMMAND ----------

from random import choice
from random import randint

###########################################################################
############################ Data placeholders ############################
###########################################################################
first_name = ["SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "MILLER", "DAVIS", "GARCIA", "RODRIGUEZ", "WILSON", "MARTINEZ", "ANDERSON", "TAYLOR", "THOMAS", "HERNANDEZ", "MOORE"]
last_name = ["JACKSON", "THOMPSON", "WHITE", "LOPEZ", "LEE", "GONZALEZ", "HARRIS", "CLARK", "LEWIS", "ROBINSON", "WALKER", "PEREZ", "HALL", "YOUNG", "ALLEN", "SANCHEZ"]
countries = [("Holy See (Vatican City State)", "VA"), ("Singapore", "SG"), ("United States", "US"), ("Iceland", "IS"), ("India", "IN"), ("China", "CN"), ("South Korea", "SK"), ("Italy", "IT"), ("Malaysia", "MY"), ("Vietnam", "VN")]
# age
# gender
# email
# annual salary
# years employed
# married
# number of dependents 
# databricks user
# databricks monthly spend
# databricks cloud
# databricks is admin
# databricks tier
###########################################################################
############################ Data placeholders ############################
###########################################################################

def generate_fake_data():
  """
  Generates 100,000 rows of fake data in csv format.
  example output (1 row): ('MOORE', 'CLARK', 'Holy See (Vatican City State)', 'VA', 45, 'female', 'MOORE.CLARK@outlook.com', 54898, 16, 'true', 4, 'false', 9449, 'gcp', 'false', 'premium')
  :return: Tuple
  """
  firstName = choice(first_name)
  lastName = choice(last_name)
  country, code = choice(countries)
  age = randint(20, 65)
  gender = choice(["male", "female"])
  email = f"{firstName}.{lastName}@{choice(['gmail', 'outlook', 'yahoo', 'aol'])}.com"
  annualSalary = randint(50000, 200000)
  yearsEmployed = randint(8, 20)
  isMarried = choice(["true", "false"])
  numberOfDependents = randint(0, 5)
  isDatabricksUser = choice(["true", "false"])
  databricksMonthlySpend = randint(0, 10000)
  databricksCloud = choice(["aws", "gcp", "azure"])
  isDatabricksAdmin = choice(["true", "false"])  # can be an admin, but not a user. so he/she just manages the permissions and all. 
  databricksTier = choice(["standard", "premium", "enterprise"])
  
  final_data = firstName, lastName, country, code, age, gender, email, annualSalary, yearsEmployed, isMarried, numberOfDependents, isDatabricksUser, databricksMonthlySpend, databricksCloud, isDatabricksAdmin, databricksTier
  
  return final_data 

# COMMAND ----------

print(generate_fake_data())

# COMMAND ----------

# create large chunks of csv data into s3 bucket synchronously

from time import time
import csv

  
still_valid = True  # Change to false if dont want to create dataset
raw_data_storage_location = "chunks-on-chunks"
counter_chunks_to_create = 0
total_chunks_to_create = 100  # 100 batches of csv files
total_rows_per_chunk = 1000000 # 1 million rows

while still_valid:
  timenow = int(time())
  csv_file = open(f"/dbfs/mnt/jake.wong.sg/{raw_data_storage_location}/{timenow}.csv", 'w')
  csv_writer = csv.writer(csv_file)
  csv_writer.writerow(["firstName", "lastName", "country", "code", "age", "gender", "email", "annualSalary", "yearsEmployed", "isMarried", "numberOfDependents", "isDatabricksUser", "databricksMonthlySpend", "databricksCloud", "isDatabricksAdmin", "databricksTier"])  # write the headers
  csv_data = []
  for i in range(0, total_rows_per_chunk):
    csv_data.append(generate_fake_data())  # get the fake data into list
    
  for each_row in csv_data:  
    csv_writer.writerow(each_row)
  
  counter_chunks_to_create += 1
  print("number of chunks created:", counter_chunks_to_create)
  if counter_chunks_to_create == total_chunks_to_create:
    still_valid = False

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/jake.wong.sg/chunks-on-chunks/

# COMMAND ----------


