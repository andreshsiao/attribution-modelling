from datatable import f, dt, last
import os
from datetime import datetime
from db import Database


def get_holdings_data(path, start_date):
    folder_list = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name)) and datetime.strptime(name, '%Y%m%d') >= start_date]
    factset = dt.Frame(names=['PORTID', 'DATE', 'ISIN', 'SEDOL', 'CUSIP', 'NAME', 'ASSETCLASS'])
    jepun = dt.Frame(names=['portfolioCode', 'holdingDate', 'ISIN', 'weight', 'instrumentName'])
    for folder in folder_list:
        factset_path = f'{path}/{folder}/FactSet_Holding.txt'
        jepun_path = f'{path}/{folder}/JEPUN_{folder}.txt'

        if os.path.exists(factset_path):
            factset_DT = dt.fread(factset_path, header=True)
            factset_DT = factset_DT[:, ['PORTID', 'DATE', 'ISIN', 'SEDOL', 'CUSIP', 'NAME', 'ASSETCLASS']]
            factset_DT = dt_time_format_transform(factset_DT, 'int')

            jepun_DT = dt.fread(jepun_path, header=True)
            jepun_DT = jepun_DT[:, ['portfolioCode', 'holdingDate', 'ISIN', 'weight', 'instrumentName']]

            factset = dt.rbind(factset, factset_DT)
            jepun = dt.rbind(jepun, jepun_DT)

    return factset, jepun


def dt_time_format_transform(frame:dt.Frame, from_type) -> dt.Frame:
    if from_type == 'str':
        frame[:, dt.update(DATE = 
            dt.time.ymd(dt.as_type(dt.str.slice(f.DATE, 0, 4), int), 
                        dt.as_type(dt.str.slice(f.DATE, 4, 6), int), 
                        dt.as_type(dt.str.slice(f.DATE, 6, 8), int)))]
    else:
        frame[:, dt.update(DATE=dt.as_type(f.DATE, 'str'))]
        frame[:, dt.update(DATE = 
             dt.time.ymd(dt.as_type(dt.str.slice(f.DATE, 0, 4), int), 
                         dt.as_type(dt.str.slice(f.DATE, 4, 6), int), 
                         dt.as_type(dt.str.slice(f.DATE, 6, 8), int)))]

    return frame

def save_facset_holdings_data(facset_holdings_frame, cursor):
    last_date = 0

    for i in range(facset_holdings_frame.nrows):
        row = facset_holdings_frame[i, :]
        
        # assign column values to variables
        port_id = row[0,0]
        date_ = row[0,1]
        ISIN = row[0,2]
        SEDOL = row[0,3]
        CUSIP = row[0,4]
        asset_name = row[0,5].replace("'", "''")
        asset_class = row[0,6]

        # log for tracking the process
        if date_ != last_date or last_date == 0:
            print(f'Factset holdings: {date_} processing...') 

        # SQL queries for inserting data into SQL Server DB       
        queries = (
        f"IF NOT EXISTS (SELECT * FROM factset_holdings \n"
        f"WHERE port_id='{port_id}' AND date_='{date_}' AND asset_name='{asset_name}') \n"
        f"BEGIN \n"
        f"INSERT INTO factset_holdings (port_id, date_, ISIN, SEDOL, \n"
        f"CUSIP, asset_name, asset_class) \n"
        f"VALUES ('{port_id}', '{date_}', '{ISIN}', '{SEDOL}',\n"
        f"'{CUSIP}', '{asset_name}', '{asset_class}') \n"
        F"END"
        )

        try:
            cursor.execute(queries)
            last_date = date_
        
        except Exception as err:
            error_message = f'Factset Holdings: port_id={port_id}, date={date_}, ISIN={ISIN}, name={asset_name}\n {err}\n'
            error_log += error_message

    cursor.commit()

def save_jepun_data(jepun_frame, cursor):
    last_date = 0

    for i in range(jepun_frame.nrows):
        row = jepun_frame[i, :]

        # assign column values to variables
        port_id = row[0,0]
        date_ = row[0,1]
        ISIN = row[0,2]
        wgt = row[0,3]
        asset_name = row[0,4].replace("'", "''") 

        # log for tracking the process
        if date_ != last_date or last_date == 0:
            print(f'Jepun: {date_} processing...') 

        # query for insert jepun data into DB
        queries = (
        f"BEGIN TRY \n"
        f"INSERT INTO jepun_data \n"
        f"VALUES ('{port_id}', '{date_}', '{ISIN}', \n" 
        f"'{wgt}', '{asset_name}') \n"
        f"END TRY \n"
        f"BEGIN CATCH \n"
        f"END CATCH"
        )

        try:
            cursor.execute(queries)
            last_date = date_
        
        except Exception as err:
            error_message = f'Jepun: port_id={port_id}, date={date_}, ISIN={ISIN}, name={asset_name}\n {err}\n'
            error_log += error_message

    cursor.commit() 


if __name__ == '__main__':
    RMDB_path = r'\\sptwap00004\DTIReport\\ERMFiles\\RMDB\\Upload'

    performance_db = Database()
    cursor = performance_db.cursor

    latest_date = cursor.execute('select max(date_) as md from factset_holdings').fetchval()
    print(f'Begin date: {latest_date}')

    error_log = ''

    factset, jepun = get_holdings_data(RMDB_path, latest_date)

    save_facset_holdings_data(factset, cursor)
    save_jepun_data(jepun, cursor)  

    with open('error_log.txt', 'wt') as log:
        log.write(error_log)
    
    log.close()

    performance_db._close()
    
    # print(factset)
    # print(jepun)
