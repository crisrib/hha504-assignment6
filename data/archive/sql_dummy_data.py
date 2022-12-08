# Import packages
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import dbm
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random

# Login credentials
GCP_MYSQL_HOSTNAME = os.getenv('GCP_MYSQL_HOSTNAME')
GCP_MYSQL_USER = os.getenv('GCP_MYSQL_USER')
GCP_MYSQL_PASSWORD = os.getenv('GCP_MYSQL_PASSWORD')
GCP_MYSQL_DATABASE = os.getenv('GCP_MYSQL_DATABASE')

# Create connection string
connection_string_gcp = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
gc_engine = create_engine(connection_string_gcp)

# Show databases
print(gc_engine.table_names())

# Create fake data
fake = Faker()
fake_patients = [
    {
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(10)]
df_fake_patients = pd.DataFrame(fake_patients)

# Drop mrn duplicates
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])

# Insert real icd codes
icd10_codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10_codes.columns)
icd10codesShort = icd10_codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)

# Drop icd code duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')

# Insert real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000)

# Drop ndc code duplicates
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

# Insert real cpt codes
cpt_codes = pd.read_excel('/Users/cristinaribeiro/Documents/GitHub/hha504-patient-portal/data/cpt.xlsx')
list(cpt_codes.columns)
new_cpt = cpt_codes.rename(columns={'Unnamed: 0': 'Code', 'Unnamed: 1': 'Description'})
new_cpt.to_csv('data/new_cpt_codes.xlsx')
cpt_codes_sample = new_cpt.sample(n=20)

# Drop cpt code duplicates
cpt_codes_sample_use = cpt_codes_sample_use.drop_duplicates(subset=['Code'], keep='first')
cpt_codes_sample_use_2 = cpt_codes_sample_use_2.drop_duplicates(subset=['Description'], keep='first')
cpt_codes_sample_use_2.to_csv('data/new_cpt_df.xlsx')
cpt_20 = pd.read_csv('data/new_cpt_df.xlsx')

# Inserting data into tables
insertQuery = "INSERT INTO production_patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
for index, row in df_fake_patients.iterrows():
    gc_engine.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)
df_gcp = pd.read_sql_query("SELECT * FROM production_patients", gc_engine)

# Inserting fake condition data into tables
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"
startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    print("inserted row db_azure: ", index)
    gc_engine.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    if startingRow == 10:
        break
df_gcp = pd.read_sql_query("SELECT * FROM conditions", gc_engine)

# Inserting fake medication data into tables
insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"
medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    gc_engine.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    if medRowCount == 10:
        break
df_gcp = pd.read_sql_query("SELECT * FROM medications", gc_engine)

# Inserting fake treatment procedures data into tables
insertQuery = "INSERT INTO treatment_procedures (cpt_code, cpt_description) VALUES (%s, %s)"
starting = 0
for index, row in cpt_20.iterrows():
    starting += 1
    gc_engine.execute(insertQuery, (row['Code'], row['Description']))
    print("inserted row: ", index)
    if starting == 10:
        break
df_gcp = pd.read_sql_query("SELECT * FROM treatment_procedures", gc_engine)

# Inserting fake patient_conditions 
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", gc_engine)
df_conditions
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", gc_engine)
df_patients

df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
for index, row in df_patients.iterrows():
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    df_conditions_sample['mrn'] = row['mrn']
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
print(df_patient_conditions.head(20))

# Random condition to each patient
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"
for index, row in df_patient_conditions.iterrows():
    gc_engine.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

# Create fake patient_medications
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", gc_engine) 
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", gc_engine)
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])
for index, row in df_patients.iterrows():
    numMedications = random.randint(1, 5)
    df_medications_sample = df_medications.sample(n=numMedications)
    df_medications_sample['mrn'] = row['mrn']
    df_patient_medications = df_patient_medications.append(df_medications_sample)
print(df_patient_medications.head(10))

# Random medication to each patient
insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"
for index, row in df_patient_medications.iterrows():
    gc_engine.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)