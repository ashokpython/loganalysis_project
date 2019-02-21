# Udacity Log Analysis Project

This is the project from udacity NanoDegree : [Fullstack Web Developer](https://classroom.udacity.com/nanodegrees/nd004/dashboard/overview)

#### Your task is to create a reporting tool that prints out reports (in plain text) 

based on the data in the database. This reporting tool is a
Python program using the psycopg2 module to connect to the database. 

### Here is your Tasks to Answer:-
1. **What are the most popular three articles of all time?** Which
  articles have been accessed the most? Present this information as a
  sorted list with the most popular article at the top.
2. **Who are the most popular article authors of all time?** That is,
   when you sum up all of the articles each author has written, which
   authors get the most page views? Present this as a sorted list with
   the most popular author at the top.
3. **On which days did more than 1% of requests lead to errors?** The
   log table includes a column status that indicates the HTTP status
   code that the news site sent to the user's browser. (Refer to this
   lesson for more information about the idea of HTTP status codes.) 
   
### Tools to Install for Project Setup:

1. **Vagrant** - [Vagrant 2.2.3](https://releases.hashicorp.com/vagrant/2.2.3/vagrant_2.2.3_x86_64.msi)
2. **VirtualBox** - [VirtualBox](https://www.virtualbox.org/wiki/Download_Old_Builds_5_1)
3. Download vagrant setup files from **[Udacity's Github](https://github.com/udacity/fullstack-nanodegree-vm)**
This files contains all configuration (Python3, Postgresql,PIP8 etc) setup for our project
4. After downloading goto downnloads folder and extract the zip file name contans **LOGS_ANALYSIS**
5. Keep it there (or) you can move to your own project path
6. Download this project: [loganalysis_project](https://github.com/ashokpython/loganalysis_project)

### Process of executing the project:
1. Goto Vagrant folder
2. Open command prompt (or) Git Bash 
3. Run **vagrant up** - to start the commandline virtual machine for the first it may take very long time depending on your internet connection
4. Run **vagrant ssh** command to enter into the virtual machine
5. Then run the project **python loganalysis_project.py**

### Oupt that you got from the execution:
Please wait we are procession your results...

=================================
 TOP THREE ARTICLES :
=================================
(1) "Candidate is jerk, alleges rival" :: 338647 views
(2) "Bears love berries, alleges bear" :: 253801 views
(3) "Bad things gone, say good people" :: 170098 views

===============================
TOP THREE AUTHORS:
=================================
(1) Ursula La Multa :: 507594 views
(2) Rudolf von Treppenwitz :: 423457 views
(3) Anonymous Contributor :: 170098 views

=================================
DAYS WITH MORE THAN 1% ERRORS:
=================================
July 17, 2016 -- 2.3% errors
