""" Import Packages """
from matplotlib.pyplot import table
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker
import uuid
import random

load_dotenv() # Verify dotenv has loaded properly 

 """ Login into Virtual mysql"""

 #Loading mysql login credentials
MYSQL_HOSTNAME = os.getenv("HOSTNAME")
MYSQL_USER = os.getenv("USERNAME")
MYSQL_PASSWORD = os.getenv("PASSWORD")
MYSQL_DATABASE = os.getenv("DATABASE") #database is db1

#connect to mysql database
connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}/{MYSQL_DATABASE}'
connection_string

db = create_engine(connection_string)

list(db.table_names()) # List Table Names
#List: table db['conditions', 'medications', 'patient_conditions', 'patient_medications', 'patient_social_determinants', 'patient_treatments_procedures', 'patients', 'social_determinants', 'treatments_procedures']

""" Populating patient records with fake data """

fake = Faker()

fake_patient_records =  [
    {
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(50)]

df_fake_patient_records = pd.DataFrame(fake_patient_records) # Save as a Dataframe
df_fake_patients = df_fake_patient_records.drop_duplicates(subset=['mrn']) # Remobe Duplicates 
len(df_fake_patient_records) # Count of records
df_fake_patient_records.head() # Preview Table

"""  upload patient records to mysql database """

insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    db.execute(insertQuery, row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'],row['gender'], row['contact_mobile'], row['contact_home'])
    print("inserted row: " + str(index))




""" real icd10 codes """
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)

# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')
icd10codesShort_1k.head()

# upload a icd10 codes to mysql database
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"
startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow = startingRow + 1
    print("starting row: " + str(startingRow))
    db.execute(insertQuery, row['CodeWithSeparator'], row['ShortDescription'])
    print("inserted row: " + str(startingRow))
    if startingRow == 100:
        break

""" Real ndc codes """
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)

# drop duplicates from ndc codes
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')
ndc_codes_1k.head()

#upload a NDC codes to mysql database
insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"
medRow = 0
for index, row in ndc_codes_1k.iterrows():
    medRow = medRow + 1
    print("starting row: " + str(medRow))
    db.execute(insertQuery, row['PRODUCTNDC'], row['NONPROPRIETARYNAME'])
    print("inserted row: " + str(medRow))
    if medRow == 100:
        break

""" real CPT codes """
cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_codes['cpt_code'] = cpt_codes['com.medigy.persist.reference.type.clincial.CPT.code']
cpt_codes.drop(columns=['com.medigy.persist.reference.type.clincial.CPT.code'], inplace=True)
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)

# drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['cpt_code'], keep='first')
cpt_codes_1k.head()

#upload CPT codes to mysql database
insertQuery = "INSERT INTO treatments_procedures (cpt_code, cpt_description) VALUES (%s, %s)"
cptRow = 0
for index, row in cpt_codes_1k.iterrows():
    cptRow = cptRow + 1
    print("starting row: " + str(cptRow))
    db.execute(insertQuery, row['cpt_code'], row['label'])
    print("inserted row: " + str(cptRow))
    if cptRow == 100:
        break

""" real Social Determinant Loinc codes """

loinc_codes = pd.read_csv('data/social_determinants.csv', low_memory=False)

# drop duplicates from SD lonic
loinc_codes = loinc_codes.drop_duplicates(subset=['loinc_code'], keep='first')
loinc_codes.head()

# upload Social Determinants lonic cides to mysql database
insertQuery = "INSERT INTO social_determinants (loinc_code, loinc_description, loinc_category) VALUES (%s, %s,%s)"
loincRow = 0
for index, row in loinc_codes.iterrows():
    loincRow = loincRow + 1
    print("starting row: " + str(loincRow))
    db.execute(insertQuery, row['loinc_code'], row['loinc_description'], row['loinc_category'])
    print("inserted row: " + str(loincRow))
    if loincRow == 100:
        break


""" Populate patient records with Social Determinates """

#first query database for a list of patients and social determinants
df_patients = pd.read_sql_query('SELECT mrn FROM patients', con=db)
df_social_determinants = pd.read_sql_query('SELECT loinc_code FROM social_determinants', con=db)

#create a stacked dataframe of patients and social determinants
df_patient_social_determinants = pd.DataFrame(columns=['mrn', 'loinc_code'])

#for each patient, randomly select a random number of social determinants and add to dataframe
for index, row in df_patients.iterrows():
    #randomly select a list of social determinants for each patient
    df_social_determinants_sample = df_social_determinants.sample(n=random.randint(1, 10))
    #add an mrn column to the list of social determinants
    df_social_determinants_sample['mrn'] = row['mrn']
    #add patient and social determinants to dataframe
    df_patient_social_determinants = df_patient_social_determinants.append(df_social_determinants_sample)
print(df_patient_social_determinants.head(20))

#upload fake patient social determinants to mysql database
insertQuery = "INSERT INTO patient_social_determinants (mrn, loinc_code) VALUES (%s, %s)"
for index, row in df_patient_social_determinants.iterrows():
    db.execute(insertQuery, row['mrn'], row['loinc_code'])
    print("inserted row: " + str(index))

""" Populate patient records with conditions """

#first query the database for a list of all patients and conditions
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db)

#create a dataframe that is stacked and gives each patient a random number of conditions between 1 and 10

df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])

# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and place it in df_patient_conditions

for index, row in df_patients.iterrows():
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 10))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
print(df_patient_conditions.head(20))

#upload fake patient conditions to mysql database

insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"
for index,row in df_patient_conditions.iterrows():
    db.execute(insertQuery, row['mrn'], row['icd10_code'])
    print("inserted row: " + str(index))

""" Populate patient records with procedures and treatments """

#first query the database for a list of all patients and conditions
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)
df_procedures = pd.read_sql_query("SELECT cpt_code FROM treatments_procedures", db)

#create a dataframe that is stacked and gives each patient a random number of conditions between 1 and 10
df_patient_procedures = pd.DataFrame(columns=['mrn', 'cpt_code'])

# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and place it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random sample of conditions from df_conditions
    df_procedures_sample = df_procedures.sample(n=random.randint(1, 10))
    # add the mrn to the df_conditions_sample
    df_procedures_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_procedures = df_patient_procedures.append(df_procedures_sample)
print(df_patient_procedures.head(20))

#upload fake patient conditions to mysql database
insertQuery = "INSERT INTO patient_treatments_procedures (mrn, cpt_code) VALUES (%s, %s)"

for index,row in df_patient_procedures.iterrows():
    db.execute(insertQuery, row['mrn'], row['cpt_code'])
    print("inserted row: " + str(index))


""" Populate patient records with medication """

# first, lets query medications and patients to get the ids
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])

# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 10
    numMedications = random.randint(1, 10)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(10))

# now lets add random medications to each patient

insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: " + index)