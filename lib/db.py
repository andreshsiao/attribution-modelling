import pyodbc
from dataclasses import dataclass

@dataclass
class Database:
    server:str 
    driver:str 
    database:str

    def __init__(self, server:str = 'SPTWGIS00001', driver:str = 'SQL Server', database:str = 'Performance'):
        driver_str = f'DRIVER={driver};SERVER={server};DATABASE={database}'
        try:
            self.cnxn = pyodbc.connect(driver_str)

        except Exception as err:
            print(f'Error: {err}')
        
        else:
            self.cursor = self.cnxn.cursor()

    
    def __del__(self):
        self.cursor.commit()

    def _close(self):
        self.cnxn.close()


if __name__ == '__main__':
    driver = 'SQL Server'
    server = 'SPTWGIS00001'
    database = 'Performance'
    
    db = Database(server, driver, database)
    cnxn = db.cnxn
    cursor = db.cursor

    print(cursor.execute('select * from a_portinfo').fetchall())