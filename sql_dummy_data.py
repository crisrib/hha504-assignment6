# Import package
import sqlite3

# Connect to sqlite
connect = sqlite3.connect('./Patients.db')

# Create database object
db = connect.cursor()

# Delete patient_table if it already exists
db.execute("DROP TABLE IF EXISTS patient_table")
connect.commit()

# Create table
table = """ CREATE TABLE patient_table (
    mrn VARCHAR(25) NOT NULL,
    firstname CHAR(25) NOT NULL,
    lastname CHAR(25) NOT NULL,
    dob CHAR(25) NOT NULL,
    sex CHAR(25) NOT NULL,
    contact VARCHAR(25) NOT NULL,
    insurance CHAR(25) NOT NULL, 
    provider CHAR(25) NOT NULL ); """

table 

db.execute(table)
connect.commit()

# Insert data into table
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('83746', 'Sam', 'Rocket', '01/18/1965', 'Female', '631-888-9999', 'BCBS', 'Dugger')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('28364', 'Timothy', 'Washington', '10/17/1999', 'Male', '631-111-2222', 'BCBS', 'Jenkins')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('33826', 'Daisy', 'Daniels', '06/23/1988', 'Female', '631-333-4444', 'Aetna', 'Dugger')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('76555', 'Chase', 'Merrick', '02/18/2001', 'Male', '631-555-6666', 'Cigna', 'Dugger')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('82763', 'David', 'Paone', '03/22/1999', 'Male', '631-999-0000', 'Magnacare', 'Polka')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('37626', 'Cole', 'Muller', '09/07/2003', 'Male', '631-444-1111', 'Cigna', 'Polka')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('58766', 'Maria', 'Okang', '01/11/1973', 'Female', '631-777-6666', 'Aetna', 'Jenkins')")
db.execute("INSERT INTO patient_table(mrn, firstname, lastname, dob, sex, contact, insurance, provider) values('12544', 'Betty', 'Vadicherla', '05/01/2010', 'Female', '631-111-7777', 'BCBS', 'Dugger')")

connect.commit()

# Close connection
connect.close()