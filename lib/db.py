from dataclasses import dataclass
import pyodbc
from dataclasses import dataclass

@dataclass
class Database:
    server:str
    driver:str
    database:str

    def __init__(self, server:str, driver:str, database:str):
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
