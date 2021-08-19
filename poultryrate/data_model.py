import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import delete
from sqlalchemy import update
from sqlalchemy.engine.url import URL
from sqlalchemy.types import BIGINT
from datetime import datetime, timedelta
import os
import configparser
from pyfcm import FCMNotification

class data_model() :
    
    #def create_connection(self):
    #     db_host="smartfarmdb.cldvyav9mexp.us-east-1.rds.amazonaws.com"
    #     db_port="admin"
    #     db_name="twint"
    #     db_username="admin"
    #     db_password="4w0pqsMWkZsXi4Piuk4L"
    #     host="192.168.18.61"
    #     port="5000"
    #     driver="mysql"
    #     self.db_url="mysql+pymysql://"+ db_username +":"+ db_password +"@"+ db_host +"/"+ db_name
    #     sqlEngine = create_engine(self.db_url, pool_recycle=3600, encoding="utf8")
    #     return sqlEngine
 
    def create_connection(self):
        driver="mysql"
        self.db_url="mysql+pymysql://"+os.environ['db_username']+":"+os.environ['db_password']+"@"+os.environ['db_host']+"/"+os.environ['db_name']
        sqlEngine = create_engine(self.db_url, pool_recycle=3600, encoding="utf8")
        return sqlEngine

    def insert_breeder_rate(self, var_date, var_city, var_rate, var_tweet_id):        
        print("insertion breeder rate ", var_date, var_city, var_rate, var_tweet_id)

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM breeder_rate_table where"
            +" date like '"+var_date[0:10]+"%%' "
            +" and city='"+var_city.capitalize()+"' "
            +" and breeder_culling_rate="+ str(var_rate)+" "
            , dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex) 
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("Breeder Rate already found")
            return

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            breeder_rate_table = Table('breeder_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_breeder_rate_table = insert(breeder_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "breeder_culling_rate": var_rate,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_breeder_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()


    def insert_doc_rate(self, var_date, var_city, var_rate, var_tweet_id):
        print("insertion doc rate ", var_date, var_city, var_rate, var_tweet_id)
        
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM doc_rate_table where"
            +" date like '"+var_date[0:10]+"%%' "
            +" and city='"+var_city.capitalize()+"' "
            +" and doc_rate="+ str(var_rate)+" "
            , dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex) 
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("doc rate already found")
            return

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            doc_rate_table = Table('doc_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_doc_rate_table = insert(doc_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "doc_rate": var_rate,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_doc_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()
    

    def insert_egg_rate(self, var_date, var_city, var_cage_rate, var_floor_rate, var_starter_rate, var_tweet_id):        
        print("insertion egg rate ", var_date, var_city, var_cage_rate, var_floor_rate, var_starter_rate, var_tweet_id)
  
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        try:
            query="SELECT * FROM egg_rate_table where" +\
            " date like '"+var_date[0:10]+"%%' "+\
            " and city='"+var_city.capitalize()+"' "+\
            " and egg_cage_rate="+ str(var_cage_rate)+" "+\
            " and egg_floor_rate="+ str(var_floor_rate)+" "+\
            " and egg_starter_rate="+ str(var_starter_rate)+" "
            print(query)
            single_tweet = pd.read_sql(query, dbConnection)

        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("egg rate already found")
            return

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            egg_rate_table = Table('egg_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_egg_rate_table = insert(egg_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "egg_cage_rate": var_cage_rate,
                "egg_floor_rate": var_floor_rate,
                "egg_starter_rate": var_starter_rate,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_egg_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()
            
    def insert_farm_rate(self, var_date, var_city, var_rate, var_tweet_id):
        print("insertion farm rate ", var_date, var_city, var_rate, var_tweet_id)
  
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        query="SELECT * FROM farm_rate_table where"+\
            " date like '"+str(var_date[0:10])+"%%' "+\
            " and city='"+var_city.capitalize()+"' "+\
            " and farm_rate="+ str(var_rate)+" "
        print(query)
        try:
            single_tweet = pd.read_sql(query, dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)  
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("farm rate already found")
            return
        
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            farm_rate_table = Table('farm_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_farm_rate_table = insert(farm_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "farm_rate": var_rate,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_farm_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()


    def insert_layer_culling_rate(self, var_date, var_city, var_cage_rate, var_floor_rate, var_tweet_id):        
        print("insertion layer culling rate ", var_date, var_city, var_cage_rate, var_floor_rate, var_tweet_id)
  
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM layer_culling_rate_table where"
            +" date like '"+var_date[0:10]+"%%' "
            +" and city='"+var_city.capitalize()+"' "
            +" and layer_culling_cage_rate="+ str(var_cage_rate)+" "
            +" and layer_culling_floor_rate="+ str(var_floor_rate)+" "
            , dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)  
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("doc rate already found")
            return
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            layer_culling_rate_table = Table('layer_culling_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_layer_culling_rate_table = insert(layer_culling_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "layer_culling_cage_rate": var_cage_rate,
                "layer_culling_floor_rate": var_floor_rate,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_layer_culling_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()



    def insert_mandi_rate(self, var_date, var_city, var_farm_rate, var_mandi_open, var_mandi_close, var_tweet_id):
        print("insertion mandi rate ", var_date, var_city, var_farm_rate, var_mandi_open, var_mandi_close, var_tweet_id)
  
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM mandi_rate_table where"
            +" date like '"+var_date[0:10]+"%%' "
            +" and city='"+var_city.capitalize()+"' "
            +" and farm_rate="+ str(var_farm_rate)+" "
            +" and mandi_open="+ str(var_mandi_open)+" "
            +" and mandi_close="+ str(var_mandi_close)+" "
            , dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)  
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("doc rate already found")
            return
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            mandi_rate_table = Table('mandi_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_mandi_rate_table = insert(mandi_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "farm_rate": var_farm_rate,
                "mandi_open": var_mandi_open,
                "mandi_close": var_mandi_close,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_mandi_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()

    def insert_supply_rate(self, var_date, var_city, var_rate, var_tweet_id):
        print("insertion supply rate ", var_date, var_city, var_rate, var_tweet_id)
        
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet = pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM supply_rate_table where"
            +" date like '"+var_date[0:10]+"%%' "
            +" and city='"+var_city.capitalize()+"' "
            +" and rate="+ str(var_rate)+" "
            , dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)  
        finally:
            dbConnection.close()

        if len(single_tweet) > 0 :
            print("doc rate already found")
            return
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        try:
            metadata = MetaData(bind=sqlEngine)
            supply_rate_table = Table('supply_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_supply_rate_table = insert(supply_rate_table).values({
                "date": var_date, 
                "city": var_city.capitalize(),
                "rate": var_rate,
                "tweet_id": var_tweet_id
                })

            dbConnection.execute(insert_supply_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            dbConnection.close()

    def fetch_rates(self, method, user_id=None, city=None, start_date=None, end_date=None, 
    tweet_id=None, days_ago=None):
        query ="SELECT * FROM city_table, "
        if "breederrate" in method:
            query += "breeder_rate_table "
        elif "docrate" in method:
            query += "doc_rate_table "
        elif "eggrate" in method:
            query += "egg_rate_table "
        elif "farmrate" in method:
            query += "farm_rate_table "
        elif "layercullingrate" in method:
            query += "layer_culling_rate_table "
        elif "mandirate" in method:
            query += "mandi_rate_table "
        else:
            return None
        query+="WHERE city = city_table.english_name AND "
        to_filter = {}

        if tweet_id:
            query += "tweet_id=%(id)s AND "
            to_filter["tweet_id"]=id
            
        if city:
            query += "city='%(city)s' AND "
            to_filter["city"]=city.capitalize()
        if start_date and end_date:
            query += "date between '%(start_date)s' and '%(end_date)s' AND "
            to_filter["start_date"]=start_date
            to_filter["end_date"]=end_date
        if tweet_id:
            query += "tweet_id=%(tweet_id)s AND "
            to_filter["tweet_id"]=tweet_id
        if days_ago:
            start_date = datetime.today() - timedelta(days=int(days_ago)+1)
            end_date = datetime.today() + timedelta(days=1)
            
            query += "date between '%(start_date)s' and '%(end_date)s' AND "
            to_filter["start_date"]=start_date
            to_filter["end_date"]=end_date
        
        query = query[:-5]

        if not (id or city or start_date or end_date or tweet_id or days_ago):
            return None

    
        query += " order by date desc "

        query_modulus = query % to_filter    
        print (query_modulus)


        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        tweets = None
        try:
            tweets = pd.read_sql(query_modulus, dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print(query_modulus)   
        finally:
            dbConnection.close()
        return tweets

    def update_processed_tweet(self, id, english_translation=None, urdu_translation=None, cities=None, processed=1):
        #sqlEngine = create_engine(self.db_url+'?charset=utf8')
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        
        query = "UPDATE tweets_table "
        query += " SET processed = "+str(processed)
        
        if english_translation and urdu_translation:
            query +=", translate_english = '"+english_translation+"', "
            query +=" translate_urdu = '"+urdu_translation+"' "
        
        if cities:
            query +=", cities = '"+cities+"' "
        
        query +=" WHERE id = " + str(id) 

        try:
            dbConnection.execute(query)
            print(query)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("query executed successfully") 
        finally:
            dbConnection.close()

    def fetch_unlabel_tweet(self, var_limit):
    
        tweet = pd.DataFrame()
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        try:
            tweet = pd.read_sql("SELECT * FROM tweets_table "+
            " where label='' order by date desc limit "+var_limit, dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("SELECT * FROM tweets_table where label='' limit "+var_limit)   
        finally:
            dbConnection.close()
        return tweet
    
    def update_label_table(self, tweet):
        
        sqlEngine = self.create_connection()
        
        dbConnection = sqlEngine.connect()

        labelTableName="label_temporary_table"
        tweets_sub_table = tweet
        tweets_sub_table["cities"]=tweets_sub_table["cities"].astype(str)
        tweets_sub_table["numbers"]=tweets_sub_table["numbers"].astype(str)
        tweets_sub_table.to_sql(labelTableName, dbConnection, if_exists='replace')

        query = "UPDATE tweets_table, label_temporary_table" + \
                " SET tweets_table.label = label_temporary_table.label ," + \
                " tweets_table.score = label_temporary_table.score" + \
                " WHERE tweets_table.id = label_temporary_table.id" 
        try:
            dbConnection.execute(query)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex) 
        finally:
            dbConnection.close()
        
        
    
    def store_latest_tweets_into_db(self, latest_tweets):
    
        tempTableName = "tweets_temporary_table"         
        tableName = "tweets_table"
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()


        try:
            frame = latest_tweets.to_sql(tempTableName, dbConnection, if_exists='replace');
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("Table "+tempTableName+" created successfully.");   
        finally:
            dbConnection.close()
            
        dbConnection = sqlEngine.connect()
        try:
            dbConnection.execute('INSERT IGNORE INTO '+tableName+' SELECT * FROM '+tempTableName)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print('INSERT IGNORE INTO '+tableName+' SELECT * FROM '+tempTableName+' successful.')
            
        finally:
            dbConnection.close()
    
    def translate_unprocessed_tweet(self):          
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        #tweets=pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM tweets_table where label !='' and processed='0' " + \
            "ORDER BY date DESC LIMIT 1", dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("Tweet is fetched for processing");   
        finally:
            dbConnection.close()

        if len(single_tweet) == 0 :
            return
        
        data_model_obj = data_model()

        if single_tweet["label"][0] ==  "doc_rate" :
            print("doc rate")
            dataframe=data_model_obj.fetch_rates("docrate", tweet_id=single_tweet["id"][0])

            english_tweet="TODAY DOC RATE ANNOUNCEMENT"
            urdu_tweet = "آج ایک دن کے چوزے کا نرخ"  

            cities=""

            for i in range(len(dataframe)):
                english_tweet +="\n"+ dataframe["english_name"][i] + " " + str(dataframe["doc_rate"][i]) 
                urdu_tweet +="\n"+ dataframe["urdu_name"][i] + " " + str(dataframe["doc_rate"][i])
                cities += dataframe["english_name"][i] + "\n"
            
            print(english_tweet)
            print(urdu_tweet)
            if len(dataframe) > 0 :
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities)
            else:
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, 2)
                    
        elif single_tweet["label"][0] ==  "egg_rate" :
            print("egg rate")
            dataframe=data_model_obj.fetch_rates("eggrate", tweet_id=single_tweet["id"][0])
            print(single_tweet["tweet"][0])
            english_tweet="TODAY EGG RATE ANNOUNCEMENT"
            urdu_tweet =" ایگز (پیٹی) کا ریٹ "
            cities=""
            
            for i in range(len(dataframe)):
                if dataframe["egg_cage_rate"][i] != 0 and dataframe["egg_floor_rate"][i] == 0 and dataframe["egg_starter_rate"][i] == 0:
                    english_tweet +="\n"+ dataframe["english_name"][i] + " " + str(dataframe["egg_cage_rate"][i]) 
                    urdu_tweet +="\n"+ dataframe["urdu_name"][i] + " " + str(dataframe["egg_cage_rate"][i])
                    cities += dataframe["english_name"][i] + "\n"
                elif dataframe["egg_cage_rate"][i] != 0 and dataframe["egg_floor_rate"][i] != 0 and dataframe["egg_starter_rate"][i] != 0:
                    english_tweet= dataframe["english_name"][i] + " Egg Rate Announcement"
                    urdu_tweet =dataframe["urdu_name"][i]+ " ایگز (پیٹی) کا ریٹ "

                    english_tweet +="\n"+ "Cage Rate " + str(dataframe["egg_cage_rate"][i]) 
                    urdu_tweet +="\n"+ " کیج ریٹ " + str(dataframe["egg_cage_rate"][i])
                    
                    english_tweet +="\n" + "Floor Rate " + str(dataframe["egg_floor_rate"][i]) 
                    urdu_tweet +="\n" + " فلور ریٹ " + str(dataframe["egg_floor_rate"][i])

                    english_tweet +="\n" + "Starter Rate " + str(dataframe["egg_starter_rate"][i]) 
                    urdu_tweet +="\n"+ " سٹارٹر ریٹ " + str(dataframe["egg_starter_rate"][i]) 
                    cities += dataframe["english_name"][i] + "\n"

            print(english_tweet)
            print(urdu_tweet)

            if len(dataframe) > 0 :
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities)
            else:
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, 2)

        elif single_tweet["label"][0] ==  "farm_rate" :
            print("translate farm rate")
            dataframe=data_model_obj.fetch_rates("farmrate", tweet_id=single_tweet["id"][0])
            print(single_tweet["tweet"][0])



            english_tweet="TODAY FARM RATE ANNOUNCEMENT"
            urdu_tweet = "آج کیلیے برائلر زندہ فارم نرخ اناوُنسمنٹ"
            cities=""
            count=dataframe["english_name"].count()
            unique=len(dataframe["english_name"].unique())

            print("cities names")
            print(dataframe["english_name"])
            if count > unique:
                print("multiple farm rate rows")
                max=dataframe["farm_rate"].max()
                print("maximum farm rate", max)
                dataframe_copy=dataframe

                for i in range(len(dataframe_copy)):
                    if dataframe_copy["farm_rate"][i] != max:
                        data_model_obj.delete_farm_rate(dataframe_copy["id"][i])
                        dataframe.drop([i],inplace=True)

            for i in range(len(dataframe)):
                english_tweet +="\n"+ dataframe.iloc[i,dataframe.columns.get_loc('english_name')] + \
                " " + str(dataframe.iloc[i, dataframe.columns.get_loc('farm_rate')]) 
                urdu_tweet +="\n"+ dataframe.iloc[i, dataframe.columns.get_loc("urdu_name")] + \
                " " + str(dataframe.iloc[i, dataframe.columns.get_loc("farm_rate")])
                cities += dataframe.iloc[i, dataframe.columns.get_loc("english_name")] + "\n"
            
            
            print(english_tweet)
            print(urdu_tweet)

            if len(dataframe) > 0 :
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities)
            else:
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, 2)

        elif single_tweet["label"][0] ==  "layer_culling_rate" :
            print("layer culling rate")
            dataframe=data_model_obj.fetch_rates("layercullingrate", tweet_id=single_tweet["id"][0])
            print(single_tweet["tweet"][0])
            english_tweet="TODAY LAYER CULLING PER BIRD RATE"
            urdu_tweet = "آج لیئر بریڈر تلف ریٹ فی عدد "
            cities=""    

            for i in range(len(dataframe)):
                english_tweet +="\n"+ dataframe["english_name"][i] + " Cage Rate " + str(dataframe["layer_culling_cage_rate"][i]) 
                english_tweet +="\n"+ dataframe["english_name"][i] + " Floor Rate " + str(dataframe["layer_culling_floor_rate"][i]) 
                urdu_tweet +="\n"+ dataframe["urdu_name"][i] + " کیج ریٹ "  +" "+ str(dataframe["layer_culling_cage_rate"][i])
                urdu_tweet +="\n"+  dataframe["urdu_name"][i] + " فلور ریٹ "+" "+ str(dataframe["layer_culling_floor_rate"][i]) 
                cities += dataframe["english_name"][i] + "\n"            

            print(english_tweet)
            print(urdu_tweet)

            if len(dataframe) > 0 :
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities)
            else:
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, 2)
        elif single_tweet["label"][0] ==  "mandi_rate" :
            print("mandi rate")
            dataframe=data_model_obj.fetch_rates("mandirate", tweet_id=single_tweet["id"][0])
            print(single_tweet["tweet"][0])
            english_tweet="CITY - FARM RATE (MANDI OPEN) MANDI SALE"
            urdu_tweet = "شہر - فارم ریٹ (منڈی اوپن) منڈی سیل "
            cities=""
            
            for i in range(len(dataframe)):
                english_tweet +="\n"+ dataframe["english_name"][i] +" - " + str(dataframe["farm_rate"][i]) + " (" + str(dataframe["mandi_open"][i]) +") " + str(dataframe["mandi_close"][i]) 

                urdu_tweet +="\n"+ dataframe["urdu_name"][i] +" - " + str(dataframe["farm_rate"][i]) + " (" + str(dataframe["mandi_open"][i]) +") " + str(dataframe["mandi_close"][i]) 

                cities += dataframe["english_name"][i] + "\n"

            print(english_tweet)
            print(urdu_tweet)
            if len(dataframe) > 0 :
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, )
            else:
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, 2)
        elif single_tweet["label"][0] ==  "breeder_rate" :
            print("breeder rate")
            data_model_obj.update_processed_tweet(single_tweet["id"][0], processed=2)
            dataframe=data_model_obj.fetch_rates("breederrate", tweet_id=single_tweet["id"][0])
            print(single_tweet["tweet"][0])
            english_tweet="TODAY BREEDER CULLING PER KG RATE"
            urdu_tweet = "آج بریڈر تلف ریٹ فی کلو"
            cities=""    

            for i in range(len(dataframe)):
                english_tweet +="\n"+ dataframe.iloc[i,dataframe.columns.get_loc('english_name')] + \
                " " + str(dataframe.iloc[i, dataframe.columns.get_loc('breeder_culling_rate')]) 
                urdu_tweet +="\n"+ dataframe.iloc[i, dataframe.columns.get_loc("urdu_name")] + \
                " " + str(dataframe.iloc[i, dataframe.columns.get_loc("breeder_culling_rate")])
                cities += dataframe.iloc[i, dataframe.columns.get_loc("english_name")] + "\n"


            print(english_tweet)
            print(urdu_tweet)

            if len(dataframe) > 0 :
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities)
            else:
                data_model_obj.update_processed_tweet(single_tweet["id"][0], english_tweet, urdu_tweet, cities, 2)            
        elif single_tweet["label"][0] ==  "supply_rate" :
            print("supply rate")
            data_model_obj.update_processed_tweet(single_tweet["id"][0], processed=2)
        elif single_tweet["label"][0] ==  "unknown" :
            print("unknown")
            data_model_obj.update_processed_tweet(single_tweet["id"][0], processed=2)
        elif single_tweet["label"][0] ==  "last_year" :
            print("last year")
            data_model_obj.update_processed_tweet(single_tweet["id"][0], processed=2)
        else :
            print(single_tweet["label"][0])
            data_model_obj.update_processed_tweet(single_tweet["id"][0], processed=2)

    def delete_farm_rate(self, var_id):
        
        if not (var_id):
            return None

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        tweets = None
        try:
            metadata = MetaData(bind=sqlEngine)
            farm_rate_table = Table('farm_rate_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            delete_farm_rate_table = delete(farm_rate_table).where(farm_rate_table.c.id == int(var_id))

            dbConnection.execute(delete_farm_rate_table)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)   
        finally:
            dbConnection.close()
    
    
    def latest_tweets_feed(self, page):
        page_size = 20
        start_limit = (page-1) * page_size
        end_limit= start_limit + page_size        
        
        query = "SELECT id, created_at, tweet, link, translate_english, translate_urdu, label, cities " + \
                " FROM tweets_table WHERE processed=1 order by created_at desc limit "+ str(start_limit) + "," + str(end_limit)
        
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        tweets = pd.DataFrame()
        try:
            tweets = pd.read_sql(query, dbConnection)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print(query)   
        finally:
            dbConnection.close()
        #tweets.set_index("id",inplace=True)
        return tweets

    def fetch_user(self, id):
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
        df_user = pd.DataFrame()
        try:
            df_user = pd.read_sql("SELECT * FROM user_table"
            +" where user_id='"+str(id)+"'" , dbConnection)
            dbConnection.close()
            return df_user
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        finally:
            dbConnection.close()
        return None

    def create_user(self, user_id, first_name, last_name, gender, date_of_birth, preferred_language, mobile_number):
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        try:
            metadata = MetaData(bind=sqlEngine)
            user_table = Table('user_table', metadata, autoload=True)
            # insert data via insert() construct
            # insert
            insert_user_table = insert(user_table).values({
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "date_of_birth":date_of_birth,
                "preferred_language": preferred_language,
                "mobile_number": mobile_number

            })

            dbConnection.execute(insert_user_table)
            dbConnection.close()
            return 201
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
            dbConnection.close()
            if str(ex).find("Duplicate entry"):
                return 409
            else:
                return 500
        finally:
            dbConnection.close()
        return 500

    def update_user(self, user_id, first_name=None, last_name=None, gender=None, date_of_birth=None, preferred_language=None, mobile_number=None, profession=None, organization_role=None, education=None, marital_status=None, country=None, mobile_operator=None, notification=None):
        update_values={}

        if first_name is not None:
            update_values['first_name'] = first_name

        if last_name is not None:
            update_values['last_name'] = last_name

        if gender is not None:
            update_values['gender'] = gender
            
        if date_of_birth is not None:
            update_values['date_of_birth'] = date_of_birth

        if preferred_language is not None:
            update_values['preferred_language'] = preferred_language

        if mobile_number is not None:
            update_values['mobile_number'] = mobile_number

        if profession is not None:
            update_values['profession'] = profession

        if organization_role is not None:
            update_values['organization_role'] = organization_role

        if education is not None:
            update_values['education'] = education

        if marital_status is not None:
            update_values['marital_status'] = marital_status

        if country is not None:
            update_values['country'] = country

        if notification is not None:
            update_values['notification'] = notification

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        try:
            metadata = MetaData(bind=sqlEngine)
            user_table = Table('user_table', metadata, autoload=True)

            update_user_table = update(user_table).where(user_table.c.user_id == user_id).values(update_values)
            #print(update_user_table)
            result=dbConnection.execute(update_user_table)
            dbConnection.close()
            #print("rows updated", result.rowcount)
            if result.rowcount == 1: 
                return 201
            if result.rowcount == 0:
                return 404
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
            #dbConnection.close()
            #if str(ex).find("Duplicate entry")
        finally:
            dbConnection.close()
        return 400

    def add_preferred_city(self, user_id, city):
        
        maximum_no_preferred_cities=5

        cities_df=self.fetch_preferred_cities(user_id)





        print("cities_df", cities_df)


        cities=[]
        if not cities_df.empty:
            for column in cities_df:
                preferred_city = cities_df[column][0]
                print("city", preferred_city)
                if len(preferred_city)>0 and preferred_city not in cities:
                    cities.append(preferred_city)
        
        if len(cities) >= 5: ## all five
            return 416

        if city not in cities:
            cities.append(city)
        else:
            #already in the list
            return 208
        
        cities += ["","","","","",""]
        
        print("cities list", cities)
        

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        try:
            metadata = MetaData(bind=sqlEngine)
            user_preferences_table = Table('user_preferences_table', metadata, autoload=True)        
            # insert data via insert() construct
            # insert
            insert_user_table = insert(user_preferences_table).values({
                "user_id": user_id,
                "city_1": cities[0],
                "city_2": cities[1],
                "city_3": cities[2],
                "city_4": cities[3],
                "city_5": cities[4],
            })
            #print(insert_user_table)
            on_duplicate_key_stmt = insert_user_table.on_duplicate_key_update(
                {
                    "city_1": cities[0],
                    "city_2": cities[1],
                    "city_3": cities[2],
                    "city_4": cities[3],
                    "city_5": cities[4]
                }
            )

            #print(on_duplicate_key_stmt)
            result=dbConnection.execute(on_duplicate_key_stmt)
            dbConnection.close()
            return 202
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        finally:
            dbConnection.close()
        return 400

    def remove_preferred_city(self, user_id, city):
        
        cities_df=self.fetch_preferred_cities(user_id)
        print("cities_df", cities_df)

        if len(cities_df) == 0: 
            return 404

        cities=[]
        for column in cities_df:
            preferred_city = cities_df[column][0]
            print("city", preferred_city)
            if len(preferred_city)>0:
                cities.append(preferred_city)

        if city in cities:
            cities.remove(city)
        else:
            return 404
        
        cities += ["","","","","",""]
        
        #print("cities list", cities)
        

        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        try:
            metadata = MetaData(bind=sqlEngine)
            user_preferences_table = Table('user_preferences_table', metadata, autoload=True)        
            # insert data via insert() construct
            # insert
            update_user_table = update(user_preferences_table).where(user_preferences_table.c.user_id == user_id).values({
                "city_1": cities[0],
                "city_2": cities[1],
                "city_3": cities[2],
                "city_4": cities[3],
                "city_5": cities[4],
            })
            print(update_user_table)


            #print(on_duplicate_key_stmt)
            result=dbConnection.execute(update_user_table)
            dbConnection.close()
            return 202
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        finally:
            dbConnection.close()
        return 400

    def fetch_preferred_cities(self, id):
        sqlEngine = self.create_connection()
        df_user_preference = pd.DataFrame()
        try:
            df_user_preference = pd.read_sql(
                "SELECT city_1, city_2, city_3, city_4, city_5 FROM user_preferences_table where user_id='"+str(id)+"'", 
                sqlEngine)
            return df_user_preference
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)

        return None

    def fetch_user_preferences(self, id):
        sqlEngine = self.create_connection()
        df_user_preference = pd.DataFrame()
        try:
            df_user_preference = pd.read_sql(
                "SELECT * FROM user_preferences_table where user_id='"+str(id)+"'", 
                sqlEngine)
            return df_user_preference
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)

        return None

    def fetch_all_cities(self):
        sqlEngine = self.create_connection()
        df_cities = pd.DataFrame()
        try:
            df_cities = pd.read_sql("SELECT english_name, urdu_name FROM city_table where display=1", sqlEngine)
            return df_cities
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)

        return None

    
    def fetch_tweet_not_notified(self):          
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()

        single_tweet=pd.DataFrame()
        try:
            single_tweet = pd.read_sql("SELECT * FROM tweets_table where processed=1 and notification=0 limit 0,1"
            , dbConnection)
            
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex) 
        finally:
            dbConnection.close()

        return single_tweet


    def update_tweet_notified(self, id):
        sqlEngine = self.create_connection()
        dbConnection = sqlEngine.connect()
                
        query = "UPDATE tweets_table "
        query += " SET notification = 1"    
        query +=" WHERE id = " + str(id) 

        try:
            dbConnection.execute(query)
            print(query)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("query executed successfully") 
        finally:
            dbConnection.close()

    def notify_tweet(self):
        data_model_obj=data_model()
        single_tweet =data_model_obj.fetch_tweet_not_notified()

        if len(single_tweet) == 0 :
            print("No tweet available for notification")
            return None

        cities_list_str = single_tweet["cities"][0]
        cities_list = cities_list_str.split('\n')
        print(cities_list)

        if len(cities_list) == 0 :
            print("No city found")
            return None         

        push_service = FCMNotification(api_key=os.environ['firebase_server_key'])

        for city in cities_list :
            if len(city) > 0 :
                message=single_tweet["translate_urdu"][0]
                result = push_service.notify_topic_subscribers(topic_name=city, message_body=message)
                print(city, result)

                
        data_model_obj.update_tweet_notified(single_tweet["id"][0])
        