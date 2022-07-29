from datatable import f, dt
import os
import pyodbc
from datetime import datetime


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

if __name__ == '__main__':
    RMDB_path = r'\\sptwap00004\DTIReport\\ERMFiles\\RMDB\\Upload'

    driver = 'SQL Server'
    server = 'SPTWGIS00001'
    database = 'Performance'
    driver_str = f'DRIVER={driver};SERVER={server};DATABASE={database}'
    cnxn = pyodbc.connect(driver_str)
    cursor = cnxn.cursor()

    latest_date = cursor.execute('select max(date_) as md from factset_holdings').fetchval()

    error_log = ''

    factset, jepun = get_holdings_data(RMDB_path, latest_date)

    
    for i in range(factset.nrows):
        row = factset[i, :]
        port_id = row[0,0]
        date_ = row[0,1]
        ISIN = row[0,2]
        SEDOL = row[0,3]
        CUSIP = row[0,4]
        asset_name = row[0,5].replace("'", "''")
        asset_class = row[0,6]

        queries = (
        # f"BEGIN TRY \n"
        f"IF NOT EXISTS (SELECT * FROM factset_holdings \n"
        f"WHERE port_id='{port_id}' AND date_='{date_}' AND asset_name='{asset_name}') \n"
        f"BEGIN \n"
        f"INSERT INTO factset_holdings (port_id, date_, ISIN, SEDOL, \n"
        f"CUSIP, asset_name, asset_class) \n"
        f"VALUES ('{port_id}', '{date_}', '{ISIN}', '{SEDOL}',\n"
        f"'{CUSIP}', '{asset_name}', '{asset_class}') \n"
        F"END"
        # f"END TRY \n"
        # f"BEGIN CATCH \n"
        # f"END CATCH"
        )

        print(queries)

        try:
            cursor.execute(queries)
        
        except Exception as err:
            error_message = f'port_id={port_id}, date={date_}, ISIN={ISIN}, name={asset_name}\n {err}\n'
            error_log += error_message
            continue

    for i in range(jepun.nrows):
        row = jepun[i, :]
        row[0,4] = row[0, 4].replace("'", "''")
        queries = (
        f"BEGIN TRY \n"
        f"INSERT INTO jepun_data \n"
        f"VALUES ('{row[0,0]}', '{row[0,1]}', '{row[0,2]}', '{row[0,3]}',\n"
        f"'{row[0,4]}') \n"
        f"END TRY \n"
        f"BEGIN CATCH \n"
        f"END CATCH"
        )
        cursor.execute(queries)

    cursor.commit()     

    with open('error_log.txt', 'wt') as log:
        log.write(error_log)
    
    log.close()
    
    print(factset)
    print(jepun)
