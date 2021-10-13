from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
from bs4 import BeautifulSoup
from poultryrate.data_model import data_model
import os

class epakpoultry() :
    def web_crawler_epakpoultry(self, url):
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--incognito')
        options.add_argument('--headless')
        driver = webdriver.Chrome(executable_path=os.environ['config_path']+os.environ['webdriver_name'], options=options)
        driver.get(url)
        timeout = 30
        try:
            WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.CLASS_NAME, "city")))
        except TimeoutException:
            driver.quit()

        html = driver.page_source
        soup=BeautifulSoup(html,"lxml")
        table = soup.find_all('table')
        df = pd.read_html(str(table))[0]
        return df


    type_url_dict = {
        'breeder_culling_rate':'https://epp.arcsolution.co/#/dailyrates/15/Breeder%20Culling%20Rate',
        'doc_rate':'https://epp.arcsolution.co/#/dailyrates/11/Broiler%20Day%20Old%20Chick%20%28DOC%29',
        'egg_rate': 'https://epp.arcsolution.co/#/dailyrates/12/Eggs', 
        'farm_rate': 'https://epp.arcsolution.co/#/dailyrates/10/Broiler',
        'golden_misri_rate' : 'https://epp.arcsolution.co/#/dailyrates/17/Golden%20Misri',
        'layer_culling_rate':'https://epp.arcsolution.co/#/dailyrates/16/Layer%20Culling%20Rate',
        'mandi_rate':'https://epp.arcsolution.co/#/dailyrates/14/Mandi%20Rates', 
        'supply_rate': 'https://epp.arcsolution.co/#/dailyrates/13/Retail%20Rates'
    }

    table_name_dict = {
        'breeder_culling_rate':'daily_breeder_culling_rate_table',
        'doc_rate':'daily_doc_rate_table',
        'egg_rate': 'daily_egg_rate_table', 
        'farm_rate':'daily_farm_rate_table', 
        'layer_culling_rate':'daily_layer_culling_rate_table',
        'mandi_rate':'daily_mandi_rate_table',
        'supply_rate': 'daily_supply_rate_table'
    }

    def fix_city_name(self, string):
        string=string.replace(',','')
        string=string.replace('.','')
        return string


    def fetch_update_epakpoultry_rates(self, data_type):
        crawler_epak_df = self.web_crawler_epakpoultry(self.type_url_dict[data_type])

        #print("web crawler epakpoultry")
        #print(crawler_epak_df)

        table_header={
            "City": "city",
            "Farm Rate":"farm_rate", 
            "Cash/Loading Rate":"cash_rate", 
            "Supply Rate":"supply_rate", 
            "Daily Rate":"doc_rate", 
            "Sale Rate":"sale_rate", 
            "Cage":"cage_rate", 
            "Floor":"floor_rate", 
            "Starter":"starter_rate",
            "Broiler Alive":"broiler_alive_rate",
            "Broiler Meat":"broiler_meat_rate",
            "Eggs":"eggs_dozen_rate",
            "Open":"mandi_open_rate",
            "Close":"mandi_close_rate",
            "Rate / Kg":"rate_per_kg",
            "Mix":"mix",
            "Male":"male",
            "Female":"female"
        }


        if '#' in crawler_epak_df.columns:
            crawler_epak_df.drop('#', axis=1, inplace=True)

        for index2 in range(len(crawler_epak_df.columns)):
            crawler_epak_df.rename({crawler_epak_df.columns[index2]:table_header[crawler_epak_df.columns[index2]]}, axis='columns', inplace=True)

        #print(crawler_epak_df.columns)


        values={}
        df_copy=crawler_epak_df.copy(deep=True)
        for index, row in crawler_epak_df.iterrows():
            empty=True
            for index2 in range(len(crawler_epak_df.columns)):
                if crawler_epak_df.columns[index2] != "city" and crawler_epak_df.iloc[index, index2] != 0:
                        empty=False
                        break
            if empty == True:
                #print(df_copy[index])
                df_copy.drop(index, inplace=True)

        crawler_epak_df=df_copy


        crawler_epak_df["city"]=crawler_epak_df["city"].apply(self.fix_city_name)
        #crawler_epak_df.info
        #print("fix epakpoultry table ")
        #print(crawler_epak_df)
        
        data_model_obj=data_model()
        db_table_df = data_model_obj.fetch_daily_table(self.table_name_dict[data_type])

        #print(db_table_df)
        #db_table_df.info()

        merge_df = pd.merge(crawler_epak_df, db_table_df, on=crawler_epak_df.columns.tolist(), how='outer', indicator=True)
        print("merge table")
        print(merge_df)

        for index, row in merge_df.iterrows():
            if row["_merge"] == "both":
                print("matching row for city ", row["city"])
            elif row["_merge"] == "left_only": 
                db_row = db_table_df[db_table_df.city == row["city"]]
                values={}
                for index2 in range(len(merge_df.columns)-3 ):
                    column_title = merge_df.columns[index2]
                    column_value = row[index2]
                    #print(column_title, column_value)
                    values[column_title]=column_value
                
                print("values to be inserted in daily")
                print(values)   
                data_model_obj.update_daily_rate_tables(self.table_name_dict[data_type], values, db_row)
