# HHA504: Assignment 6: Patient Portal
## Steps Taken
Cloud Environment: GCP
Step 1: Create VM
1. Click 'Create a VM instance'
2. Create VM name
Only change these settings:
3. Under ‘Machine Type’, change to ‘e2-medium’
4. Under ‘Boot Disk’ section, change operating system to ‘Ubuntu’ and its version type to ‘Ubuntu 18.04 LTS x86/64’
5. Under ‘Firewall’ section, allow BOTH HTTP and HTTPS traffic
6. Click 'Create'

Step 2: Create Firewall Rule:
1. Under the project folder, go into ‘Firewall’ dashboard
2. Click ‘Create a firewall rule’
3. Create firewall rule name
Only change these settings:
4. Under ‘Targets’, insert choose ‘All instances in the network’
5. Under ‘Source IPv4 ranges’, insert ’0.0.0.0/0’
6. Under ‘Protocols and ports’, check off the ‘TCP’ box and insert ’3306’
7. Click 'Create'

Step 3: Setup GCP terminal & Mysql
1. Go into this terminal an enter:
$ sudo apt-get update
$ sudo apt install mysql-server mysql-client
$ sudo mysql
Create user ‘dba’@‘%’ identified by ‘ahi2020’;
Select user from mysql.user;
Grant all privileges on *.* to ‘dba’@‘%’ with grant option;
Show grants for dba;

Step 4: Create sql instance
1. Go into desired project
2. Enter ’sql’ into search bar and click that option
3. Click ‘Create instance’
4. Choose ‘mysql’
5. Give sql instance a name
6. Create a password
7. Under ‘Configuration’, change to ‘Development’
8. Under ‘Machine Type’, change to ‘lightweight’ and choose ‘1vCPU’ below it
9. Click ‘Create instance’
