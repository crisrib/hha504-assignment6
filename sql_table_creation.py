""" Import Packages """
import sqlalchemy
from sqlalchemy import create_engine
import dotenv
from dotenv import load_dotenv
import os
import pandas as pd


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

""" Drop Tables if exist """

sql = "DROP TABLE IF EXISTS customers" 

""" Creating tables """

#Patient Table
table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
#Medication Table
table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

# Condition Table
table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

# Creating a Patient medication table
table_patients_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

#Creating Patient condition table
table_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""
#Creating a Social Determinates table
table_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc_code varchar(255) default null unique,
    loinc_category varchar(255) default null,
    loinc_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
#Creating a Patient Social Determinates table
table_patients_social_determinants = """
create table if not exists patient_social_determinants (
    id int auto_increment,
    mrn varchar(255) default null,
    loinc_code varchar(255) default null,
    loinc_description varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (loinc_code) REFERENCES social_determinants(loinc_code) ON DELETE CASCADE
); 
"""

#Creating a Treatments procedure Table
table_treatments_procedures = """
create table if not exists treatments_procedures (
    id int auto_increment,
    cpt_code varchar(255) default null unique,
    cpt_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

#Creating patient treatments procedure Table 
table_patients_treatments_procedures = """
create table if not exists patient_treatments_procedures (
    id int auto_increment,
    mrn varchar(255) default null,
    cpt_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (cpt_code) REFERENCES treatments_procedures(cpt_code) ON DELETE CASCADE
); 
"""
#execute queries
db.execute(table_patients)
db.execute(table_medications)
db.execute(table_conditions)
db.execute(table_social_determinants)
db.execute(table_patients_medications)
db.execute(table_patient_conditions)
db.execute(table_patients_social_determinants)
db.execute(table_treatments_procedures)
db.execute(table_patients_treatments_procedures)


#confirm tables were created
db.table_names()