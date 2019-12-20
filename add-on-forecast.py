#libraries

import pandas as pd
import numpy as np
import os
import glob
import datetime, time
import mysql.connector as mariadb
import pyodbc
import tkinter as tk
import Messages
import math
from tkinter import messagebox

my_date = datetime.date.today()
my_date_p7 = datetime.date.today() + datetime.timedelta(days=7)

year, week, day = my_date.isocalendar()
year_p7, week_p7, day_p7 = my_date_p7.isocalendar()

def yes(msg_yes):
    root = tk.Tk()
    root.quit()
    print(msg_yes)

def no(msg_no_title, msg_no):
    root = tk.Tk()
    root.destroy()
    messagebox.showinfo(msg_no_title , msg_no)
    sys.exit()

#inputs

frame = []

countries = ['NL', 'BE', 'LU', 'FR']

total_output_rows = 355

factor_lower_limit = 0.8

factor_higher_limit = 1.2

#paths

path_fc = 'G:/My Drive/NL - 02 - Operations/00. OPS-BI/01. WEEKLY OPS/'

path_sources = 'G:/My Drive/NL - 02 - Operations/00. OPS-BI/01. WEEKLY OPS/00. SOURCEFILES'

#get source files

source_sku = pd.read_excel(path_sources + '/SKUs.xlsx', sheet_name = 'SKUsNewLogic')

source_zip = pd.read_excel(path_sources + '/box_information.xlsx', sheet_name = 'zipcodes')

#connect to DWH

def connect_to_dwh():
    start = time.time()
    conn_dwh = pyodbc.connect('''Driver={Cloudera ODBC Driver for Impala};
                         Host=cloudera-impala-proxy.live.bi.hellofresh.io;
                         Port=21050;
                         AuthMech=3;
                         UID= ''' + os.environ.get('jump_cloud_id') + ''';
                         PWD= ''' + os.environ.get('jump_cloud_pwd') + ''';
                         UseASL=1;
                         SSL=1''',
                         autocommit=True)
    end = time.time()
    print ("Time elapsed to make the connection with DWH")
    print (str((end - start)*1000) + ' ms.')
    return conn_dwh

conn_dwh = connect_to_dwh()

#get boblive actuals from DWH

def boblive_addons(countries, frame):
    
    for c in countries:
    
        query = pd.read_sql_query('''
        WITH bs AS

        (SELECT bs1.subscription_id, fk_geography
        FROM
        
        (SELECT sd1.subscription_id, MAX(hellofresh_delivery_week) AS hellofresh_delivery_week
        FROM fact_tables.boxes_shipped AS fbs1
        LEFT JOIN dimensions.subscription_dimension AS sd1 ON fk_subscription = sk_subscription
        WHERE fbs1.country = "''' + c +
        '''" GROUP BY sd1.subscription_id) AS bs1
        
        LEFT JOIN
        
        (SELECT sd2.subscription_id, fk_geography, hellofresh_delivery_week
        FROM fact_tables.boxes_shipped AS fbs2
        LEFT JOIN dimensions.subscription_dimension AS sd2 ON fk_subscription = sk_subscription
        WHERE fbs2.country = "''' + c +
        '''" GROUP BY sd2.subscription_id, fk_geography, hellofresh_delivery_week) AS bs2
        
        ON bs1.subscription_id = bs2.subscription_id AND bs1.hellofresh_delivery_week = bs2.hellofresh_delivery_week),
        
        addons AS
        
        (SELECT addsc.country AS country, addsc.week AS week, sd.subscription_id AS subscription_id, pd.product_sku AS product_sku, qpc.index AS index, qpc.quantity AS quantity, max(event_unix_timestamp) AS time_stamp
        FROM fact_tables.addons_selection_changed addsc,  addsc.quantity_per_course qpc
        LEFT JOIN dimensions.subscription_dimension as sd on fk_subscription = sk_subscription
        LEFT JOIN dimensions.product_dimension as pd on fk_product = sk_product
        WHERE addsc.country = "''' + c +
        '''" AND (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS VARCHAR(10))
                    END)
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 7))
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 14))
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 21))
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 28)))
        GROUP BY addsc.country, addsc.week, subscription_id, pd.product_sku, qpc.index, qpc.quantity),

        addon_unique AS

        (SELECT addsc.country AS country, addsc.week AS week, sd.subscription_id AS subscription_id, pd.product_sku AS product_sku, MAX(event_unix_timestamp) AS time_stamp
        FROM fact_tables.addons_selection_changed addsc,  addsc.quantity_per_course qpc
        LEFT JOIN dimensions.subscription_dimension as sd on fk_subscription = sk_subscription
        LEFT JOIN dimensions.product_dimension as pd on fk_product = sk_product
        WHERE addsc.country = "''' + c +

        '''" AND (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 WEEK)) AS VARCHAR(10))
                    END)
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 7))
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 14))
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 21))
        OR (addsc.week = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10))
                    END)
        AND DAYOFYEAR(FROM_UNIXTIME(addsc.event_unix_timestamp))  < (DAYOFYEAR(CURRENT_TIMESTAMP()) - 28)))

        GROUP BY addsc.country, addsc.week, subscription_id, pd.product_sku)

        SELECT  addons.country, geo.zip_code, addons.week, addons.subscription_id, addons.product_sku, addons.index, addons.quantity, addons.time_stamp
        FROM addons 
        LEFT JOIN bs ON addons.subscription_id = bs.subscription_id
        LEFT JOIN dimensions.geography_dimension AS geo ON bs.fk_geography = sk_geography
        INNER JOIN addon_unique ON addons.country = addon_unique.country AND addons.week = addon_unique.week AND addons.subscription_id = addon_unique.subscription_id AND addons.product_sku = addon_unique.product_sku AND addons.time_stamp = addon_unique.time_stamp
        ''', conn_dwh)

        frame.append(query)
        boblive_addons = pd.concat(frame, ignore_index = True)

    print("The data from the DWH is collected.")

    return boblive_addons

boblive_addons = boblive_addons(countries, frame)

#yes/no check message region

msg_yes = "The addon forecast will continue while there are discrepancies in the number of addons due to the region merge."
msg_no_title = "Error message"
msg_no = "The code will stop running."

#add region to dataframe

def add_region(boblive_addons, source_zip, yes, no):
    count_total_addons = boblive_addons['week'].count() #count of total addons

    #add FL and WA regions
    be_addons = boblive_addons[boblive_addons['country'] == "BE"]
    be_addons['zipcode2'] = be_addons['country'] + be_addons['zip_code']
    be_addons = pd.merge(be_addons, source_zip, how = 'left', left_on = ['zipcode2'], right_on = ['PC'])
    be_addons['region'] = be_addons['NL_FL_WA']
    
    #ratio FL and WA not empty regions
    be_addons_not_empty = be_addons[~be_addons['region'].isnull()]
    fl = be_addons_not_empty[be_addons_not_empty.region == "FL"]
    ratio_fl = fl['region'].count()/be_addons_not_empty['region'].count()
    wa = be_addons_not_empty[be_addons_not_empty.region == "WA"]
    ratio_wa = wa['region'].count()/be_addons_not_empty['region'].count()

    #calculate split empty regions
    be_addons_empty = be_addons[be_addons['region'].isnull()]
    be_addons_empty['region'] = np.random.choice(["FL", "WA"], p=[ratio_fl, ratio_wa], size=len(be_addons_empty))
    
    be_addons = be_addons_empty.append(be_addons_not_empty)
    count_be_addons = be_addons['week'].count()

    #add NL, LU and FR region
    nl_lu_fr_addons = boblive_addons[boblive_addons['country'] != "BE"]
    nl_lu_fr_addons['region'] = nl_lu_fr_addons['country']
    count_nl_lu_fr_addons = nl_lu_fr_addons['week'].count() #count NL, LU and FR addons

    boblive_addons = be_addons.append(nl_lu_fr_addons)
    count_boblive_addons = boblive_addons['week'].count()

    if (count_total_addons - count_boblive_addons) != 0:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(count_total_addons - count_current_addons) + " deleted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    print("There are no discrepancies after adding the region.")
    boblive_addons = boblive_addons[['country', 'region', 'week', 'subscription_id', 'product_sku', 'index', 'quantity']]
    
    return boblive_addons

boblive_addons = add_region(boblive_addons, source_zip, yes, no)

#yes/no message group boblive addons

msg_yes = "The addon forecast will continue while there are discrepancies in the number of addons due to a group by function."
msg_no_title = "Error message"
msg_no = "The code will stop running."

#group boblive addons

def group_by_addon(boblive_addons, yes, no):

    #group subscription id's of boblive addons
    count_boblive_addons = boblive_addons['week'].count()
    boblive_addons = boblive_addons.groupby(['week', 'country', 'region', 'product_sku', 'index', 'quantity'], as_index = False).count()
    sum_subscription_id = boblive_addons['subscription_id'].sum()

    if (count_boblive_addons - sum_subscription_id) != 0:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(count_boblive_addons - sum_subscription_id) + " deleted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    print("There are no discrepancies after the sum of subscription id's.")

    boblive_addons['total_bob'] = boblive_addons['subscription_id']
    return boblive_addons

boblive_addons = group_by_addon(boblive_addons, yes, no)

#yes/no check message source sku

msg_yes = "The addon forecast will continue even though there are wrong sku-mealswap combinations in BOB."
msg_no_title = "Check"
msg_no = "Check the unknown sku and mealswap combos in BOB. The code will stop running."

#add days, persons and recipe numbers to dataframe

def add_source_sku(boblive_addons, source_sku, yes, no):
    #create sku_combo column
    boblive_addons['sku_combo'] = boblive_addons['product_sku'].astype(str) + boblive_addons['index'].astype(str) + ":" + boblive_addons['quantity'].astype(str)

    #check if there are wrong mealswap - sku combinations
    boblive_addons['check'] = boblive_addons['sku_combo'].isin(source_sku['sku_combo'])
    wrong_combos = boblive_addons[(boblive_addons['check'] != True)]
    count_wrong_combos = wrong_combos['check'].count()

    #error message if wrong mealswap - sku combinations are found
    if count_wrong_combos > 1:

        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(count_wrong_combos) + " unknown sku and mealswap combos.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    #merge boblive addons with sku source file
    boblive_addons = pd.merge(boblive_addons, source_sku, how = 'left', left_on = ['sku_combo'], right_on = ['sku_combo'])

    #check defaults in the merged file
    boblive_addons['default'] = boblive_addons['default'].astype(str)
    count_addons_start = boblive_addons['week'].count()
    boblive_addons = boblive_addons[boblive_addons['default'] != 'nan']
    count_addons_end = boblive_addons['week'].count()

    #error message if wrong defaults are found
    msg_yes = "The addon forecast will continue even though the wrong number of rows are deleted in the default check."
    msg_no_title = "Check"
    msg_no = "The code will stop running."

    if  (count_addons_start - count_addons_end) - count_wrong_combos != 0:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There will be " + str(count_addons_start - count_addons_end) + " deleted instead of " + wrong_combos + ".""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()
        print('There are ' + str(count_addons_start - count_addons_end) + ' rows removed due to a wrong sku and mealswap combination.')

    boblive_addons = boblive_addons[['week', 'country', 'region', 'product_sku', 'type', 'default', 'days', 'persons', 'total_bob']]

    print('The number of days, persons and recipes is added to the dataframe.')
    return boblive_addons

boblive_addons = add_source_sku(boblive_addons, source_sku, yes, no)

#split double mealchoice (vb 60 60 becomes two rows one withe 60 and one with 60)

def split_rows(boblive_addons):
    split = boblive_addons['default'].str.split(' ').apply(pd.Series, 1).stack()
    split.index = split.index.droplevel(-1)
    split.name = 'default'
    del boblive_addons['default']
    boblive_addons = boblive_addons.join(split)
    boblive_addons['default'] = boblive_addons['default'].astype(int)

    return boblive_addons

boblive_addons = split_rows(boblive_addons)

#group by defaults

boblive_addons = boblive_addons.groupby(['week', 'country', 'region', 'product_sku', 'type', 'default', 'days', 'persons'], as_index = False).sum()

#get actuals from DWH

def addon_actuals():
    query = pd.read_sql_query('''
    SELECT kd.week_hf, kd.country, kd.region, kd.day_name, kd.product_family, kd.mealsize, kd.meal_number, kd.meal_to_deliver_incl
    FROM uploads.bnl_opsbi_recipe_splits AS kd
    WHERE kd.day_id = "ZO_T-0_AC"
    AND kd.product_family <> "Mealbox"
    AND kd.product_family <> "mealbox"
    AND (kd.week_hf = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 0 WEEK)) AS VARCHAR(10))
                    END)
    OR kd.week_hf = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 WEEK)) AS VARCHAR(10))
                    END)
    OR kd.week_hf = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 WEEK)) AS VARCHAR(10))
                    END)
    OR kd.week_hf = CONCAT(CAST(YEAR(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10)),
                    '-W',
                    CASE
                      WHEN CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS INT) < 10 THEN CONCAT('0',CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10)))
                      ELSE CAST(WEEK(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 WEEK)) AS VARCHAR(10))
                    END))''', conn_dwh)
    
    addon_actuals = pd.DataFrame(query, columns=['week_hf', 'country', 'region', 'day_name', 'product_family', 'mealsize', 'meal_number', 'meal_to_deliver_incl'])
    print("The latest actuals from the DWH are collected.")
    return addon_actuals

addon_actuals = addon_actuals()

#yes/no check message for week actuals

msg_yes = "The addon forecast will continue even though there are wrong numbers in the week totals."
msg_no_title = "Check"
msg_no = "The code will stop running."

#group actuals on week level

def addon_actuals_week(addon_actuals, yes, no):
    sum_total_actuals = addon_actuals['meal_to_deliver_incl'].sum()
    addon_actuals_week = addon_actuals.groupby(['week_hf', 'country', 'region', 'product_family', 'mealsize','meal_number'], as_index = False)['meal_to_deliver_incl'].sum()
    sum_actuals_week = addon_actuals_week['meal_to_deliver_incl'].sum()

    if (sum_total_actuals - sum_actuals_week) != 0:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(sum_total_actuals - sum_actuals_week) + " deleted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    addon_actuals_week['total_meals_one_week'] = addon_actuals_week['meal_to_deliver_incl']
    addon_actuals_week = addon_actuals_week.drop(columns = ['meal_to_deliver_incl'])
    return addon_actuals_week

addon_actuals_week = addon_actuals_week(addon_actuals, yes, no)

#higher and lower limits based on actuals latest 4 weeks

def limits(addon_actuals_week):
    #calculate lower limit per sku
    lower_limit = addon_actuals_week.groupby(['country', 'region', 'product_family', 'mealsize','meal_number'], as_index = False)['total_meals_one_week'].min()
    lower_limit['lower_limit'] = lower_limit['total_meals_one_week'] * factor_lower_limit
    lower_limit['lower_limit'] = lower_limit['lower_limit'].apply(np.floor)
    lower_limit = lower_limit[['country', 'region', 'product_family', 'mealsize','meal_number', 'lower_limit']]

    #calculate higher limit per sku
    higher_limit = addon_actuals_week.groupby(['country', 'region', 'product_family', 'mealsize','meal_number'], as_index = False)['total_meals_one_week'].max()
    higher_limit['higher_limit'] = higher_limit['total_meals_one_week'] * factor_higher_limit
    higher_limit['higher_limit'] = higher_limit['higher_limit'].apply(np.ceil)
    higher_limit = higher_limit[['country', 'region', 'product_family', 'mealsize','meal_number', 'higher_limit']]

    return lower_limit, higher_limit

lower_limit, higher_limit = limits(addon_actuals_week)

#yes/no check message for day distribution

msg_yes = "The addon forecast will continue even though there are wrong numbers due to the day distribution calculation."
msg_no_title = "Check"
msg_no = "The code will stop running."

#calculate day distribution

def day_distribution(addon_actuals_week, addon_actuals, yes, no):
    #sum total meals before calculation day distribution
    count_addon_actuals = addon_actuals['week_hf'].count()

    #calculate day distribution
    addon_actuals_day = pd.merge(addon_actuals, addon_actuals_week, how = 'left', left_on = ['week_hf' , 'country', 'region', 'product_family', 'mealsize', 'meal_number'], right_on = ['week_hf' , 'country', 'region', 'product_family', 'mealsize', 'meal_number'])
    addon_actuals_day['day_distribution'] = np.where(addon_actuals_day['total_meals_one_week'] == 0, 0, addon_actuals_day['meal_to_deliver_incl']/addon_actuals_day['total_meals_one_week'])
    addon_actuals_day = addon_actuals_day.groupby(['country', 'region', 'day_name', 'product_family', 'mealsize','meal_number'], as_index = False)['day_distribution'].mean()

    #calculate meals after calculation day distriubtion
    count_addon_actuals_day = addon_actuals_day['country'].count()*4

    if (count_addon_actuals - count_addon_actuals_day) != 0:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(count_addon_actuals - count_addon_actuals_day) + " deleted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    return addon_actuals_day

addon_day_distribution = day_distribution(addon_actuals_week, addon_actuals, yes, no)

#yes/no check message for combined historic boblive and actuals

msg_yes = "The addon forecast will continue even though there are wrong numbers due to the merge of the historic boblive data and the actuals."
msg_no_title = "Check"
msg_no = "The code will stop running."

#compare boblive with actuals of past 4 weeks

def combine_boblive_with_actuals(boblive_addons,addon_actuals_week, yes, no):
    if week_p7 < 10:
        week_fc = "0" + str(week_p7)
    else:
        week_fc = week_p7

    #filter on actuals before current week
    past_boblive = boblive_addons[boblive_addons['week'] != (str(year_p7) + '-W' + str(week_fc))]
    count_past_boblive = past_boblive['week'].count()

    #merge boblive history with actuals
    boblive_actuals = pd.merge(past_boblive, addon_actuals_week, how = 'left', left_on = ['week', 'country', 'region', 'default', 'persons'], right_on = ['week_hf', 'country', 'region', 'meal_number', 'mealsize'])
    boblive_actuals = boblive_actuals[['week', 'country', 'region', 'type', 'persons', 'days', 'meal_number', 'total_bob', 'total_meals_one_week']]
    boblive_actuals['growth rate'] = boblive_actuals['total_meals_one_week']/boblive_actuals['total_bob']
    boblive_actuals_avg_growth = boblive_actuals.groupby(['country','region', 'type', 'persons', 'days', 'meal_number'], as_index = False)['growth rate'].mean()

    #calculate rows after merge
    count_boblive_actuals = boblive_actuals_avg_growth['country'].count()*4

    if (count_past_boblive - count_boblive_actuals) != 0:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(count_addon_actuals - count_addon_actuals_day) + " deleted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    return boblive_actuals_avg_growth

boblive_actuals = combine_boblive_with_actuals(boblive_addons, addon_actuals_week, yes, no)

# yes/no check message for forecast calculations

msg_yes = "The addon forecast will continue even though something went wrong in the final forecast calculation."
msg_no_title = "Check"
msg_no = "The code will stop running."

#calculate forecast for the upcoming week

def forecast(boblive_actuals, boblive_addons, addon_day_distribution, yes, no):  
    if week_p7 < 10:
        week_fc = "0" + str(week_p7)
    else:
        week_fc = week_p7

    #filter the current boblive data
    current_boblive = boblive_addons[boblive_addons['week'] == (str(year_p7) + '-W' + str(week_fc))]

    #calculate forecasted addons
    forecast = pd.merge(current_boblive, boblive_actuals, how = 'left', left_on = ['country', 'region', 'default', 'persons', 'days','type'], right_on = ['country', 'region', 'meal_number', 'persons', 'days', 'type'])
    forecast['forecasted_addons'] = forecast['total_bob'] * forecast['growth rate']
    forecast = forecast[['week', 'country', 'region', 'type', 'persons', 'days', 'meal_number', 'total_bob', 'growth rate','forecasted_addons']]

    #calculate day distribution
    forecast['forecasted_addons'] = forecast['forecasted_addons'].astype(float)
    sum_forecasted_addons = forecast['forecasted_addons'].sum()
    forecast = pd.merge(addon_day_distribution, forecast, how = 'left', left_on = ['country', 'region', 'mealsize', 'meal_number'], right_on = ['country', 'region', 'persons', 'meal_number'])
    forecast['forecasted_addons'] = forecast['day_distribution'] * forecast['forecasted_addons']
    forecast['forecasted_addons'] = forecast['forecasted_addons'].astype(float)

    #replace empty values with 0 and fill in correct weeknumber for empty values (empty values are not yet found in Bob)
    forecast['forecasted_addons'] = np.where(forecast['forecasted_addons'].isnull() == True, 0, forecast['forecasted_addons'])

    forecast['week'] = np.where(forecast['week'].isnull() == True, str(year_p7) + '-W' + str(week_fc), forecast['week'])

    sum_forecasted_addons_day = forecast['forecasted_addons'].sum()

    if (sum_forecasted_addons - sum_forecasted_addons_day) < -1 or (sum_forecasted_addons - sum_forecasted_addons_day) > 1:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(sum_forecasted_addons - sum_forecasted_addons_day) + " deleted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    #final dataframe clean up
    forecast['forecasted_addons'] = forecast['forecasted_addons'].apply(np.ceil)
    forecast['locale'] = np.where(forecast.region.str.contains("NL"), "NLD",
                         np.where(forecast.region.str.contains("FL"), "NLD",
                         np.where(forecast.region.str.contains("WA"), "FRA",
                         np.where(forecast.region.str.contains("LU"), "FRA", "FRA"))))
    forecast = forecast.rename(columns={'day_name':'day2', 'mealsize': 'size', 'forecasted_addons':'meals_to_deliver'})

    #remove old logic sku's
    forecast = forecast.drop(forecast[(forecast['product_family'] == "Breakfastbox") & (forecast['size'] == 4)].index)
    forecast = forecast.drop(forecast[(forecast['product_family'] == "Fruitbox") & (forecast['size'] == 4)].index)

    #final dataframes
    forecast_info = forecast[['week', 'country', 'region', 'locale','day2', 'product_family', 'size', 'meal_number','total_bob', 'growth rate','meals_to_deliver']]
    forecast_info['total_bob'] = np.where(forecast_info['total_bob'].isnull() == True, 0, forecast_info['total_bob'])
    forecast_info['growth rate'] = np.where(forecast_info['growth rate'].isnull() == True, 0, forecast_info['growth rate'])
    forecast = forecast[['week', 'country', 'region', 'locale','day2', 'product_family', 'size', 'meal_number','meals_to_deliver']]

    #check total number of rows
    count_rows = forecast['week'].count()

    if count_rows != total_output_rows:
        root = tk.Tk()
        root.lift()
        root.geometry('500x100+470+350')
        root.wm_title("Error Message")
        msgFrame = tk.Label(root, text="There are " + str(total_output_rows - count_rows) + " not in the output file.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
        msgFrame.place(x=100 , y=20)
        buttonClick1 = tk.Button(root, text="Yes", command=yes)
        buttonClick2 = tk.Button(root, text="No", command=no)
        buttonClick1.place(x=200 , y=60)
        buttonClick2.place(x=300 , y=60)
        root.mainloop()

    return forecast, forecast_info

forecast, forecast_info = forecast(boblive_actuals, boblive_addons, addon_day_distribution, yes, no)

# yes/no check message for lower and higher limits

msg_yes = "The addon forecast will continue even though the forecast exceeds the lower and higher limits."
msg_no_title = "Check"
msg_no = "The code will stop running."

#check lower and higher limits

def check_limits(forecast_info, lower_limit, higher_limit, path_fc, yes, no):
    #compare the forecast with the higher and lower limits
    forecast_info = forecast_info.groupby(['week','country', 'region', 'product_family', 'size', 'meal_number', 'total_bob', 'growth rate'], as_index = False)['meals_to_deliver'].sum()
    check_forecast = pd.merge(forecast_info, lower_limit, how = 'left', left_on = ['country', 'region', 'product_family','size', 'meal_number'], right_on = ['country', 'region', 'product_family', 'mealsize', 'meal_number'])
    check_forecast = pd.merge(check_forecast, higher_limit, how = 'left', left_on = ['country', 'region', 'product_family', 'size', 'meal_number'], right_on = ['country', 'region', 'product_family', 'mealsize', 'meal_number'])
    check_forecast['check_lower'] = np.where(check_forecast['meals_to_deliver'] < check_forecast['lower_limit'], "BAD", "GOOD")
    check_forecast['check_higher'] = np.where(check_forecast['meals_to_deliver'] > check_forecast['higher_limit'], "BAD", "GOOD")
    check_forecast = check_forecast[['week','country', 'region', 'product_family','size', 'meal_number','total_bob', 'growth rate','meals_to_deliver', 'lower_limit', 'higher_limit', 'check_lower', 'check_higher']]

    if week_p7 < 10:
        week_fc = "0" + str(week_p7)
    else:
        week_fc = week_p7

    #export check for comparison
    if not os.path.exists(path_fc):
        messagebox.showinfo("Error message", "The forecast path does not exist. The code will stop running.")
        sys.exit()

    else:
        check_forecast.to_csv('' + path_fc + str(year_p7) + '-W' + str(week_fc) + '/01. WEEKLY FORECAST/01. ADD_ONS/day_' + str(day) + '_check_lower_higher_limits.csv', sep = ';', encoding = 'utf-8-sig' ,
                         index = None) 
        path_fc = os.path
        print ('The csv file with the lower and higher limit checks is uploaded.')

    #count rows with a forecast lower or higher than the set limits
    under_forecast = check_forecast[check_forecast.check_lower == "BAD"]
    count_under_forecast = under_forecast['country'].count()
    over_forecast = check_forecast[check_forecast.check_higher == "BAD"]
    count_over_forecast = over_forecast['country'].count()

    if count_under_forecast != 0 or count_over_forecast != 0:
       root = tk.Tk()
       root.lift()
       root.geometry('500x100+470+350')
       root.wm_title("Error Message")
       msgFrame = tk.Label(root, text="There are addons which are under or over forecasted.""\n Do you still want to continue?", font = ('TkDefaultFont', 10))
       msgFrame.place(x=100 , y=20)
       buttonClick1 = tk.Button(root, text="Yes", command=yes)
       buttonClick2 = tk.Button(root, text="No", command=no)
       buttonClick1.place(x=200 , y=60)
       buttonClick2.place(x=300 , y=60)
       root.mainloop()

    return check_forecast

check_limits = check_limits(forecast_info, lower_limit, higher_limit, path_fc, yes, no)

def export_forecast(forecast, path_fc):
    if week_p7 < 10:
        week_fc = "0" + str(week_p7)
    else:
        week_fc = week_p7

    if not os.path.exists(path_fc):
        messagebox.showinfo("Error message", "The forecast path does not exist. The code will stop running.")
        sys.exit()

    else:
        forecast.to_csv('' + path_fc + str(year_p7) + '-W' + str(week_fc) + '/01. WEEKLY FORECAST/01. ADD_ONS/day_' + str(day) + '_addon_forecast.csv', sep = ';', encoding = 'utf-8-sig' ,
                         index = None) 
        path_fc = os.path
        print ('The csv files with the addon forecast has been uploaded.')
        messagebox.showinfo("Done", "Finished King.")
        sys.exit()
        
export_forecast = export_forecast(forecast, path_fc)
