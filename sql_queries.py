import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('db_config.cfg')


# DROP TABLES
staging_crime = "DROP TABLE IF EXISTS staging_crime;"
fact_crime = "DROP TABLE IF EXISTS fact_crime;"
dim_auto_theft = "DROP TABLE IF EXISTS dim_auto_theft;"
dim_serious_fraud = "DROP TABLE IF EXISTS dim_serious_fraud;"
dim_victims_of_rape = "DROP TABLE IF EXISTS dim_victims_of_rape;"
dim_property_stolen_and_recovered = "DROP TABLE IF EXISTS dim_property_stolen_and_recovered;"
dim_complaints_against_police = "DROP TABLE IF EXISTS dim_complaints_against_police;"


# CREATE TABLES
staging_crime_create = ("""CREATE TABLE staging_crime(    
    area_name VARCHAR,
    auto_theft_coordinated_traced INTEGER,
    auto_theft_recovered INTEGER,
    auto_theft_stolen INTEGER,
    calendar_year INTEGER,
    cases_property_recovered INTEGER,
    cases_property_stolen INTEGER,
    cpa_cases_registered INTEGER,
    cpa_cases_reported_for_dept_action INTEGER,
    cpa_cases_sent_for_trials_chargesheeted INTEGER,
    cpa_complaints_cases_declared_false_unsubstantiated INTEGER,
    cpa_complaints_received_alleged INTEGER,
    cpa_no_of_departmental_enquiries INTEGER,
    cpa_no_of_judicial_enquiries INTEGER,
    cpa_no_of_magisterial_enquiries INTEGER,
    cpb_police_personnel_acquitted INTEGER,
    cpb_police_personnel_cases_withdrawn_or_otherwise_disposed_of INTEGER,
    cpb_police_personnel_convicted INTEGER,
    cpb_police_personnel_sent_up_for_trial INTEGER,
    cpb_police_personnel_trial_completed INTEGER,
    cpc_police_personnel_cases_trial_completed INTEGER,
    cpc_police_personnel_cases_withdrawn_or_otherwise_disposed_of INTEGER,
    cpc_police_personnel_disciplinary_action_initiated INTEGER,
    cpc_police_personnel_dismissal_removal_from_service INTEGER,
    cpc_police_personnel_major_punishment_awarded INTEGER,
    cpc_police_personnel_minor_punishment_awarded INTEGER,
    df_auto_theft VARCHAR,
    df_complaints_against_police VARCHAR,
    df_property_stolen_and_recovered VARCHAR,
    df_serious_fraud VARCHAR,
    df_victims_of_rape VARCHAR,
    group_name VARCHAR,
    loss_of_property_10_25_crores INTEGER,
    loss_of_property_1_10_crores INTEGER,
    loss_of_property_25_50_crores INTEGER,
    loss_of_property_50_100_crores INTEGER,
    loss_of_property_above_100_crores INTEGER,
    sub_group_name VARCHAR,
    value_of_property_recovered INTEGER,
    value_of_property_stolen INTEGER,
    victims_above_50_yrs INTEGER,
    victims_between_1014_yrs INTEGER,
    victims_between_1418_yrs INTEGER,
    victims_between_1830_yrs INTEGER,
    victims_between_3050_yrs INTEGER,
    victims_of_rape_total INTEGER,
    victims_upto_10_yrs INTEGER)""")


fact_crime_create = ("""CREATE TABLE fact_crime(
    id_fact_crime                       INTEGER     IDENTITY(0,1)   PRIMARY KEY,
    calendar_year                       INTEGER     NOT NULL SORTKEY,
    group_name                          VARCHAR     NOT NULL,
    sub_group_name                      VARCHAR     NOT NULL)""")

dim_auto_theft_create = ("""CREATE TABLE dim_auto_theft(
    id_auto_theft                   INTEGER     IDENTITY(1,1)   PRIMARY KEY,
    area_name                       VARCHAR     NOT NULL, 
    calendar_year                   INTEGER     NOT NULL SORTKEY,
    group_name                      VARCHAR     NOT NULL,
    sub_group_name                  VARCHAR     NOT NULL,
    auto_theft_coordinated_traced   INTEGER     NOT NULL,
    auto_theft_recovered            INTEGER     NOT NULL,
    auto_theft_stolen               INTEGER     NOT NULL)""")

dim_serious_fraud_create = ("""CREATE TABLE dim_serious_fraud(
    id_serious_fraud                    INTEGER     IDENTITY(0,1)   PRIMARY KEY,
    area_name                           VARCHAR     NOT NULL, 
    calendar_year                       INTEGER     NOT NULL SORTKEY,
    group_name                          VARCHAR     NOT NULL,
    sub_group_name                      VARCHAR     NOT NULL,
    loss_of_property_1_10_crores        INTEGER     NOT NULL,
    loss_of_property_10_25_crores       INTEGER     NOT NULL,
    loss_of_property_25_50_crores       INTEGER     NOT NULL,
    loss_of_property_50_100_crores      INTEGER     NOT NULL,
    loss_of_property_above_100_crores   INTEGER     NOT NULL)""")

dim_victims_of_rape_create = ("""CREATE TABLE dim_victims_of_rape(
    id_victims_of_rape            INTEGER     IDENTITY(0,1)   PRIMARY KEY,
    area_name                     VARCHAR     NOT NULL, 
    calendar_year                 INTEGER     NOT NULL SORTKEY,
    group_name                    VARCHAR     NOT NULL,
    sub_group_name                VARCHAR     NOT NULL,
    victims_above_50_yrs          INTEGER     NOT NULL,           
    victims_between_1014_yrs      INTEGER     NOT NULL,      
    victims_between_1418_yrs      INTEGER     NOT NULL,           
    victims_between_1830_yrs      INTEGER     NOT NULL,           
    victims_between_3050_yrs      INTEGER     NOT NULL,           
    victims_of_rape_total         INTEGER     NOT NULL,           
    victims_upto_10_yrs           INTEGER     NOT NULL)""")

dim_property_stolen_and_recovered_create = ("""CREATE TABLE dim_property_stolen_and_recovered(
    id_property_stolen_and_recovered    INTEGER     IDENTITY(0,1)   PRIMARY KEY,
    area_name                           VARCHAR     NOT NULL, 
    calendar_year                       INTEGER     NOT NULL SORTKEY,
    group_name                          VARCHAR     NOT NULL,
    sub_group_name                      VARCHAR     NOT NULL,
    cases_property_recovered            INTEGER     NOT NULL,           
    cases_property_stolen               INTEGER     NOT NULL,           
    value_of_property_recovered         INTEGER     NOT NULL,           
    value_of_property_stolen            INTEGER     NOT NULL)""")

dim_complaints_against_police_create = ("""CREATE TABLE dim_complaints_against_police(
    id_complaints_against_policeid                                  INTEGER     IDENTITY(0,1)   PRIMARY KEY,
    area_name                                                       VARCHAR     NOT NULL, 
    calendar_year                                                   INTEGER     NOT NULL SORTKEY,
    group_name                                                      VARCHAR     NOT NULL,
    sub_group_name                                                  VARCHAR     NOT NULL,
    cpa_cases_registered                                            VARCHAR     NOT NULL,           
    cpa_cases_reported_for_dept_action                              VARCHAR     NOT NULL,           
    cpa_complaints_cases_declared_false_unsubstantiated             VARCHAR     NOT NULL,           
    cpa_complaints_received_alleged                                 VARCHAR     NOT NULL,           
    cpa_no_of_departmental_enquiries                                VARCHAR     NOT NULL,           
    cpa_no_of_magisterial_enquiries                                 VARCHAR     NOT NULL,           
    cpa_cases_sent_for_trials_chargesheeted                         VARCHAR     NOT NULL,           
    cpa_no_of_judicial_enquiries                                    VARCHAR     NOT NULL,           
    cpb_police_personnel_acquitted                                  VARCHAR     NOT NULL,           
    cpb_police_personnel_convicted                                  VARCHAR     NOT NULL,           
    cpb_police_personnel_sent_up_for_trial                          VARCHAR     NOT NULL,           
    cpb_police_personnel_trial_completed                            VARCHAR     NOT NULL,           
    cpb_police_personnel_cases_withdrawn_or_otherwise_disposed_of   VARCHAR     NOT NULL,           
    cpc_police_personnel_cases_trial_completed                      VARCHAR     NOT NULL,           
    cpc_police_personnel_cases_withdrawn_or_otherwise_disposed_of   VARCHAR     NOT NULL,           
    cpc_police_personnel_disciplinary_action_initiated              VARCHAR     NOT NULL,           
    cpc_police_personnel_dismissal_removal_from_service             VARCHAR     NOT NULL,           
    cpc_police_personnel_major_punishment_awarded                   VARCHAR     NOT NULL,           
    cpc_police_personnel_minor_punishment_awarded                   VARCHAR     NOT NULL)""")


# STAGING TABLES
staging_crime_copy = ("""
    copy staging_crime from {data_bucket}
    access_key_id ''
    secret_access_key ''     
    IGNOREHEADER 1
    DELIMITER ','
    """).format(data_bucket=config['S3']['CRIME_DATA'])


# FINAL TABLES
col_select1 = """calendar_year, group_name, sub_group_name"""

fact_crime_insert = (f"""INSERT INTO fact_crime({col_select1})
    SELECT DISTINCT {col_select1} 
    FROM staging_crime;""")

col_select2 = """area_name, calendar_year, group_name, 
    sub_group_name, auto_theft_coordinated_traced, auto_theft_recovered,
    auto_theft_stolen"""

dim_auto_theft_insert = (f"""INSERT INTO dim_auto_theft({col_select2})
    SELECT DISTINCT {col_select2} FROM staging_crime WHERE df_auto_theft = 'Yes';
    """)

col_select3 = """area_name, 
    calendar_year, group_name, sub_group_name, cpa_cases_registered, 
    cpa_cases_reported_for_dept_action, 
    cpa_complaints_cases_declared_false_unsubstantiated,
    cpa_complaints_received_alleged, cpa_no_of_departmental_enquiries,
    cpa_no_of_magisterial_enquiries, cpa_cases_sent_for_trials_chargesheeted,
    cpa_no_of_judicial_enquiries, cpb_police_personnel_acquitted, 
    cpb_police_personnel_convicted, cpb_police_personnel_sent_up_for_trial,
    cpb_police_personnel_trial_completed, 
    cpb_police_personnel_cases_withdrawn_or_otherwise_disposed_of,
    cpc_police_personnel_cases_trial_completed,
    cpc_police_personnel_cases_withdrawn_or_otherwise_disposed_of,
    cpc_police_personnel_disciplinary_action_initiated,
    cpc_police_personnel_dismissal_removal_from_service,
    cpc_police_personnel_major_punishment_awarded,
    cpc_police_personnel_minor_punishment_awarded"""

dim_complaints_against_police_insert = (f"""
    INSERT INTO dim_complaints_against_police ({col_select3})
    SELECT  DISTINCT {col_select3}
    FROM staging_crime WHERE df_complaints_against_police = 'Yes';""")

col_select4 = """area_name, calendar_year, group_name, sub_group_name, 
    loss_of_property_1_10_crores, loss_of_property_10_25_crores,
    loss_of_property_25_50_crores,loss_of_property_50_100_crores,
    loss_of_property_above_100_crores"""

dim_serious_fraud_insert = (f"""
    INSERT INTO dim_serious_fraud ({col_select4})
    SELECT DISTINCT {col_select4} 
    FROM staging_crime WHERE df_serious_fraud = 'Yes';""")

col_select5 = """area_name, calendar_year, 
    group_name, sub_group_name, cases_property_recovered, 
    cases_property_stolen, value_of_property_recovered, 
    value_of_property_stolen"""

dim_property_stolen_and_recovered_insert = (f"""
    INSERT INTO dim_property_stolen_and_recovered ({col_select5})
    SELECT  DISTINCT {col_select5}
    FROM staging_crime 
    WHERE df_property_stolen_and_recovered = 'Yes';""")

col_sel = """area_name, calendar_year, group_name, 
sub_group_name, victims_above_50_yrs, victims_between_1014_yrs, 
victims_between_1418_yrs, victims_between_1830_yrs,
victims_between_3050_yrs, victims_of_rape_total, victims_upto_10_yrs"""

dim_victims_of_rape_insert = (f"""
    INSERT INTO dim_victims_of_rape ({col_sel})
    SELECT  DISTINCT {col_sel}
    FROM staging_crime 
    WHERE df_victims_of_rape = 'Yes';""")


# QUERY LISTS to create table
create_table_queries = [staging_crime_create, fact_crime_create,
                        dim_auto_theft_create, dim_serious_fraud_create,
                        dim_victims_of_rape_create,
                        dim_property_stolen_and_recovered_create,
                        dim_complaints_against_police_create]

# QUERY LISTS to drop table
drop_table_queries = [staging_crime, fact_crime, dim_auto_theft,
                      dim_serious_fraud, dim_victims_of_rape,
                      dim_property_stolen_and_recovered,
                      dim_complaints_against_police]

# QUERY LISTS to copy staging table
copy_table_queries = [staging_crime_copy]

# QUERY LISTS to insert into table
insert_table_queries = [dim_complaints_against_police_insert,
                        dim_auto_theft_insert,
                        dim_serious_fraud_insert,
                        dim_property_stolen_and_recovered_insert,
                        dim_victims_of_rape_insert, fact_crime_insert]
