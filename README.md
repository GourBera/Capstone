# **Crime in India**


### **Project Summary**

Setting up a Redshift data warehouse about 'Crime in India' on Amazon Cloud. In-order to formulate various economical and financial decision it is important to analyze socio economic and crime related information of particular geographic location. Which provide a 360 degree overview about any particular area. The dataset contains complete information about various aspects of crimes happened in India from 2001. There are many factors that can be analysed from this dataset

There could be many things one can understand by analyzing this dataset. Few inspirations for you to start with.
    >> Crime by place of occurrence.
    >> Anti corruption cases vs arrests.
    >> Which state is the safest for foreigners?
    >> Juveniles family background, education and economic setup. etc.

### Project Description**

>> Build an ETL pipeline for a database hosted on Redshift

>> Load data from S3 to staging tables on Redshift and execute SQL statements 

>> Create various analytics tables from staging tables



### **Project Datasets**
> Two datasets that reside in Directory Folder, consists of five different crime data set

>> 1. Auto Theft (Stolen & Recovered)

    >> Motor Cycles/ Scooters

    >> Motor Car/Taxi/Jeep

    >> Buses

    >> Goods carrying vehicles (Trucks/Tempo etc)

    >> Other Motor vehicles

    >> Total (Sum of 1-5 Above)

>> 2. Complaints/Cases Against Police Personnel

    >> Complaints Received/Cases Registered

    >> Police Personnel Involved/Action Taken

    >> Departmental Action/Punishments
    
>> 3. Property Stolen & Recovered (Crime Head)

    >> Dacoity

    >> Robbery

    >> Burglary

    >> Theft

    >> Criminal Breach of Trust

    >> Other Property

    >> Total Property Stolen & Recovered
        
>> 4.  Serious Fraud

    >> Criminal Breach of Trust

    >> Cheating
    
>> 5. Victims of Rape(Age Group-wise)

    >> Incest Rape Cases

    >> Other Rape Cases (Other than Incest)

    >> Total Rape Cases
    
    
### **Schema for Table design**

#### **Fact Table**

> **fact_crime**
>> - id
>> - year
>> - calendar_year
>> - group_name
>> - sub_group_name
>> - id_auto_theft
>> - id_complaints_against_policeid
>> - id_property_stolen_and_recovered
>> - id_serious_fraud
>> - id_victims_of_rape


#### **Dimension Tables**

> **dim_auto_theft**
>> - id
>> - area_name
>> - calendar_year
>> - group_name
>> - sub_group_name
>> - auto_theft_coordinated_traced
>> - auto_theft_recovered
>> - auto_theft_stolen


> **dim_complaints_against_police**
>> - id_complaints_against_policeid
>> - area_name
>> - calendar_year
>> - group_name
>> - sub_group_name
>> - cpa_cases_registered
>> - cpa_cases_reported_for_dept._action
>> - cpa_complaints_cases_declared_false_unsubstantiated
>> - cpa_complaints_received_alleged
>> - cpa_no_of_departmental_enquiries
>> - cpa_no_of_magisterial_enquiries
>> - cpa_cases_sent_for_trials_chargesheeted
>> - cpa_no_of_judicial_enquiries
>> - cpb_police_personnel_acquitted
>> - cpb_police_personnel_convicted
>> - cpb_police_personnel_sent_up_for_trial
>> - cpb_police_personnel_trial_completed
>> - cpb_police_personnel_cases_withdrawn_or_otherwise_disposed_of
>> - cpc_police_personnel_cases_trial_completed
>> - cpc_police_personnel_cases_withdrawn_or_otherwise_disposed_of
>> - cpc_police_personnel_disciplinary_action_initiated
>> - cpc_police_personnel_dismissal_removal_from_service
>> - cpc_police_personnel_major_punishment_awarded
>> - cpc_police_personnel_minor_punishment_awarded


> **dim_property_stolen_and_recovered**
>> - id_property_stolen_and_recovered
>> - area_name
>> - calendar_year
>> - group_name
>> - sub_group_name
>> - cases_property_recovered
>> - cases_property_stolen
>> - value_of_property_recovered
>> - value_of_property_stolen


> **dim_serious_fraud**
>> - id_serious_fraud
>> - area_name
>> - calendar_year
>> - group_name
>> - sub_group_name
>> - loss_of_property_1_10_crores
>> - loss_of_property_10_25_crores
>> - loss_of_property_25_50_crores
>> - loss_of_property_50_100_crores
>> - loss_of_property_above_100_crores


> **dim_victims_of_rape**
>> - id_victims_of_rape
>> - area_name
>> - calendar_year
>> - group_name
>> - sub_group_name
>> - victims_above_50_yrs
>> - victims_between_1014_yrs
>> - victims_between_1418_yrs
>> - victims_between_1830_yrs
>> - victims_between_3050_yrs
>> - victims_of_rape_total
>> - victims_upto_10_yrs


### **Project Template**

> Project workspace includes four files:

>> **create_tables.py** - Create fact and dimension tables for the star schema in Redshift.

>> **etl.py** - Load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.

>> **sql_queries.py** - Define SQL statements, which will be imported into the two other files above.

>> **README.md** - provides project description about process and decisions for this ETL pipeline.


### **Project Steps**

> **Create Table Schemas**
>> 1. Design schemas for fact and dimension tables
>> 2. Write SQL statement for each of these tables in sql_queries.py
>> 3. Complete the logic in create_tables.py to connect to the database and create these tables
>> 4. Write SQL statements to drop tables in the beginning of create_tables.py if the tables already exist. 
>> 5. Launch a redshift cluster and create an IAM role that has read access to S3.
>> 6. Add redshift database and IAM role info to dwh.cfg.
>> 7. Test by running create_tables.py and verifying the table schemas in redshift database using query Editor in the AWS Redshift console.

> **Build ETL Pipeline**
>> 1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
>> 2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
>> 3. Test by running etl.py after running create_tables.py and running the analytic queries on Redshift database to compare the results with the expected results.
>> 4. Delete redshift cluster when Completed.


The project follows the follow steps:

    Step 1: Scope the Project and Gather Data
    Step 2: Explore and Assess the Data
    Step 3: Define the Data Model
    Step 4: Run ETL to Model the Data
    Step 5: Complete Project Write Up

Step 2: Explore and Assess the Data
Explore the Data

Identify data quality issues, like missing values, duplicate data, etc.
Cleaning Steps

Document steps necessary to clean the data

Step 3: Define the Data Model
3.1 Conceptual Data Model

Map out the conceptual data model and explain why you chose that model
3.2 Mapping Out Data Pipelines

List the steps necessary to pipeline the data into the chosen data model
Step 4: Run Pipelines to Model the Data
4.1 Create the data model

Build the data pipelines to create the data model.

# Write code here

4.2 Data Quality Checks

Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:

    Integrity constraints on the relational database (e.g., unique key, data type, etc.)
    Unit tests for the scripts to ensure they are doing the right thing
    Source/Count checks to ensure completeness

Run Quality Checks

# Perform quality checks here

4.3 Data dictionary

Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.
Step 5: Complete Project Write Up

    Clearly state the rationale for the choice of tools and technologies for the project.
    Propose how often the data should be updated and why.
    Write a description of how you would approach the problem differently under the following scenarios:
        The data was increased by 100x.
        The data populates a dashboard that must be updated on a daily basis by 7am every day.
        The database needed to be accessed by 100+ people.

