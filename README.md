# Assesment Combined CSV
Task : Combined all the csv files in the folder called "Engineering Test Risk Analytics" to Combined.csv files along with some constraints. There are two files in the repository :
1.	ProcessFiles (A package can be run in terminal or Python supported IDE)
2.	Airflow Combined CSV - a dag file to schedule the task

#ProcessFiles:

* After cloning github repository to local to run the file please use this command in the terminal (Make sure to run this command from ProcessFiles folder:
-python src/ProcessIp/run_process.py

* Next to run test cases run below commands:
-pytest src/ProcessIp/Tests


#Airflow Combined CSV

* To run this make sure you have airflow set up - https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html
* Either please clone this repo in the file where other dags or airflow files are
* Once you have this Airflow Combined_CSV_DAG in dag folder run airflow
* dag will be seen in the airflow UI
* Please change the Variable in Admin tab to add path of the file where your csv files resides (as shown in the image)
* Once Variable is set Trigger the dag - it is scheduled to run daily but the time can be changed in the python file in "schedule" variable of dag

<img width="1304" alt="Screen Shot 2022-08-04 at 11 29 51 PM" src="https://user-images.githubusercontent.com/42661704/182995575-319f351d-8024-4082-bc6e-8e3f3c0b51fa.png">
