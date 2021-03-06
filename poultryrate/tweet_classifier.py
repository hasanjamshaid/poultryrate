'''
#Step 3: Process all tweets and assign them ‘label’
'''

import os
import pandas as pd
import re
import numpy as np
from poultryrate.data_model import data_model



class tweet_classifier():


    tweet_summary = pd.DataFrame()
    tweet_exploded = pd.DataFrame()
    islamicmonths = pd.DataFrame()
    gregorianmonths = pd.DataFrame()
    daysofweek = pd.DataFrame()
    words_breeder_rate = pd.DataFrame()
    words_doc_rate = pd.DataFrame()
    words_egg_rate = pd.DataFrame()
    words_farm_rate = pd.DataFrame()
    words_last_year = pd.DataFrame()
    words_layer_culling_rate = pd.DataFrame()
    words_mandi_rate = pd.DataFrame()
    words_obituary = pd.DataFrame()
    words_supply_rate = pd.DataFrame()
    words_extra = pd.DataFrame()
    words_junk = pd.DataFrame()
    cities = pd.DataFrame()


    def __init__(self):

        self.islamicmonths=pd.read_csv(os.environ['config_path']+'islamicmonths.txt', sep=':')
        self.gregorianmonths=pd.read_csv(os.environ['config_path']+'gregorianmonths.txt', sep=':')
        self.daysofweek=pd.read_csv(os.environ['config_path']+'daysofweek.txt', sep=':')
        self.words_breeder_rate=pd.read_csv(os.environ['config_path']+'words_breeder_rate.txt', sep=':')
        self.words_doc_rate=pd.read_csv(os.environ['config_path']+'words_doc_rate.txt', sep=':')
        self.words_egg_rate=pd.read_csv(os.environ['config_path']+'words_egg_rate.txt', sep=':')
        self.words_farm_rate=pd.read_csv(os.environ['config_path']+'words_farm_rate.txt', sep=':')
        self.words_last_year=pd.read_csv(os.environ['config_path']+'words_last_year.txt', sep=':')
        self.words_layer_culling_rate=pd.read_csv(os.environ['config_path']+'words_layer_culling_rate.txt', sep=':')
        self.words_mandi_rate=pd.read_csv(os.environ['config_path']+'words_mandi_rate.txt', sep=':')
        self.words_obituary=pd.read_csv(os.environ['config_path']+'words_obituary.txt', sep=':')
        self.words_supply_rate=pd.read_csv(os.environ['config_path']+'words_supply_rate.txt', sep=':')
        self.words_extra=pd.read_csv(os.environ['config_path']+'words_extra.txt', sep=':')
        self.words_junk=pd.read_csv(os.environ['config_path']+'words_junk.txt', sep=':')
        self.cities=pd.read_csv(os.environ['config_path']+'cities.txt', sep=':')        

    def tweets_features_engineering(self, tweet, row):
        

        txt=tweet.at[row, "tweet"].lower()

        city_list=list()


        for i in range(len(self.cities)):
            if self.cities.iloc[i,1] in txt:
                city_list.append(self.cities.iloc[i,0])
        
        for i in range(len(self.cities)):
            if self.cities.iloc[i,0].lower() in txt:
                city_list.append(self.cities.iloc[i,0])
            
        
        if len(city_list) > 0:
            tweet.at[row, "cities"] = np.unique(city_list)
            tweet.at[row, "cities_len"] = len(tweet.at[row, "cities"])

        words_breeder_rate_count=0
        for i in range(len(self.words_breeder_rate)):
            if self.words_breeder_rate.iloc[i,1] in txt:
                words_breeder_rate_count += 1
        tweet.at[row, "words_breeder_rate"] = words_breeder_rate_count   

        words_doc_rate_count=0
        for i in range(len(self.words_doc_rate)):
            if self.words_doc_rate.iloc[i,1] in txt:
                words_doc_rate_count += 1
        tweet.at[row, "words_doc_rate"] = words_doc_rate_count  

        words_egg_rate_count=0
        for i in range(len(self.words_egg_rate)):
            if self.words_egg_rate.iloc[i,1] in txt:
                words_egg_rate_count += 1
        tweet.at[row, "words_egg_rate"] = words_egg_rate_count   

        words_farm_rate_count=0
        for i in range(len(self.words_farm_rate)):
            if self.words_farm_rate.iloc[i,1] in txt:
                words_farm_rate_count += 1
        tweet.at[row, "words_farm_rate"] = words_farm_rate_count   


        words_last_year_count=0
        for i in range(len(self.words_last_year)):
            if self.words_last_year.iloc[i,1] in txt:
                words_last_year_count += 1
        tweet.at[row, "words_last_year"] = words_last_year_count   


        words_layer_culling_rate_count=0
        for i in range(len(self.words_layer_culling_rate)):
            if self.words_layer_culling_rate.iloc[i,1] in txt:
                words_layer_culling_rate_count += 1
        tweet.at[row, "words_layer_culling_rate"] = words_layer_culling_rate_count   


        words_mandi_rate_count=0
        for i in range(len(self.words_mandi_rate)):
            if self.words_mandi_rate.iloc[i,1] in txt:
                words_mandi_rate_count += 1
        tweet.at[row, "words_mandi_rate"] = words_mandi_rate_count   


        words_obituary_count=0
        for i in range(len(self.words_obituary)):
            if self.words_obituary.iloc[i,1] in txt:
                words_obituary_count += 1
        tweet.at[row, "words_obituary"] = words_obituary_count   


        words_supply_rate_count=0
        for i in range(len(self.words_supply_rate)):
            if self.words_supply_rate.iloc[i,1] in txt:
                words_supply_rate_count += 1
        tweet.at[row, "words_supply_rate"] = words_supply_rate_count   
        
        words_extra_count=0
        for i in range(len(self.words_extra)):
            if self.words_extra.iloc[i,1] in txt:
                words_extra_count += 1
        tweet.at[row, "extra"] = words_extra_count   

        words_junk_count=0
        for i in range(len(self.words_junk)):
            if self.words_junk.iloc[i,0] in txt:
                words_junk_count += 1
        if '🍗' in txt :
            tweet.at[row, "meat"] = txt.count('🍗')

        if '🐥' in txt :
            tweet.at[row, "chick"] = txt.count('🐥')
            
        if '🥚' in txt :
            tweet.at[row, "egg"] = txt.count('🥚')

        if '🐓' in txt :
            tweet.at[row, "rooster"] = txt.count('🐓')
        
        for j in range(len(self.islamicmonths)):
            if self.islamicmonths.iloc[j,1] in txt:
                tweet.at[row, "date_in_tweet"] += 1
        for k in range(len(self.gregorianmonths)):
            if self.gregorianmonths.iloc[k,1] in txt:
                tweet.at[row, "date_in_tweet"] += 1 
        for l in range(len(self.daysofweek)):
            if self.daysofweek.iloc[l,1] in txt:
                tweet.at[row, "date_in_tweet"] += 1        

        if txt.startswith("@"):
            tweet.at[row, "junk"]=1        
                
        numbers=re.findall(r'[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?', txt)
        

        index=0
        while index < len(numbers):
            numbers[index]=numbers[index].replace(",", "")
            numbers[index]=float(numbers[index])
            if numbers[index] < 0 and index > 0:
                numbers[index] *= -1
                del numbers[index-1]
            index+=1

        tweet.at[row, "numbers"]=numbers
        tweet.at[row,"numbers_len"]=len(numbers)

        tweet.at[row,"translate"]=txt


        return tweet

    def assign_label(self, tweet):

        if os.environ['build_mode'] == 'debug' :
            
            print("tweet.words_breeder_rate", tweet.at[0, "words_breeder_rate"])
            print("tweet.words_doc_rate",tweet.at[0, "words_doc_rate"])
            print("tweet.words_egg_rate",tweet.at[0, "words_egg_rate"])
            print("tweet.words_mandi_rate",tweet.at[0, "words_mandi_rate"])
            print("tweet.words_farm_rate",tweet.at[0, "words_farm_rate"])

            print("tweet.words_last_year",tweet.at[0, "words_last_year"])
            print("tweet.words_layer_culling_rate",tweet.at[0, "words_layer_culling_rate"])
            print("tweet.words_supply_rate",tweet.at[0, "words_supply_rate"])

            print("tweet.cities_len",tweet.at[0, "cities_len"])
            print("tweet.numbers_len",tweet.at[0, "numbers_len"])
            
            print("tweet.chick",tweet.at[0, "chick"])
            print("tweet.rooster",tweet.at[0, "rooster"])
            print("tweet.egg",tweet.at[0, "egg"])


        tweet["breeder_rate"]=tweet.words_breeder_rate + tweet.cities_len + tweet.numbers_len 

        tweet["doc_rate"]=tweet.words_doc_rate + tweet.cities_len + tweet.numbers_len + tweet.chick

        tweet["egg_rate"]= tweet.words_egg_rate + tweet.cities_len + tweet.numbers_len + tweet.egg

        tweet["mandi_rate"] = tweet.words_mandi_rate + tweet.cities_len + tweet.rooster + tweet.numbers_len + tweet.egg + tweet.chick

        tweet["farm_rate"]= tweet.words_farm_rate + tweet.cities_len + tweet.rooster + tweet.numbers_len
        
        tweet["last_year"]= tweet.words_last_year + tweet.cities_len + tweet.numbers_len + tweet.rooster + tweet.egg + tweet.chick 
        
        if 'hashtags' in tweet.columns and tweet.at[0,"hashtags"].find("گزشتہ_سال") != -1 :
            tweet["last_year"] += 5

        tweet["layer_culling_rate"]= tweet.words_layer_culling_rate + tweet.cities_len + tweet.numbers_len

        tweet["supply_rate"]= tweet.words_supply_rate + tweet.cities_len + tweet.numbers_len + tweet.rooster + tweet.meat + tweet.video

        tweet["informational"]=tweet.junk + tweet.video + tweet.numbers_len

        if tweet["date_in_tweet"][0] > 0 :
            tweet["breeder_rate"]+=1
            tweet["doc_rate"]+=1
            tweet["egg_rate"]+=1
            tweet["mandi_rate"]+=1
            tweet["last_year"]+=1
            tweet["layer_culling_rate"]+=1
            tweet["supply_rate"]+=1

        tweet["obituary"]=tweet.words_obituary

        print(tweet["tweet"][0])

        score_df= tweet.loc[:, "breeder_rate":"obituary"]
        pd.set_option('display.max_columns', 40)

        if os.environ['build_mode'] == 'debug' :
            print(score_df)
        
        max_score_label=score_df.eq(score_df.max(1), axis=0).dot(score_df.columns)
        max_score_label_transpose=score_df.eq(score_df.max(1), axis=0).transpose()
        
        if os.environ['build_mode'] == 'debug' :
            print(max_score_label_transpose)
        
        label_count=max_score_label_transpose[max_score_label_transpose[0]==True].count()
        
        if os.environ['build_mode'] == 'debug' :
            print(label_count)
        if label_count[0] == 1 :
            tweet["label"] = max_score_label[0]
            tweet["score"]=score_df.max(1)[0]
        else :
            tweet["label"] = "unknown"
        
        print(" label assigned ", tweet["label"])

        return tweet

    def process_tweet_line(self, tweet):
                
        for row in range(len(tweet)):
            print("line wise ",tweet["tweet"].iloc[row])

            if tweet.at[row, "date_in_tweet"]>0 and tweet.at[row, "numbers_len"] == 2:
                tweet.at[row, "label"]="delete"

            if self.tweet_summary.label[0] == "farm_rate" and tweet.at[row, "rooster"] >= 1 and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("farm rate row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="farm_rate"
            elif self.tweet_summary.label[0] == "farm_rate" and tweet.at[row, "rooster"] >=1 and tweet.at[row, "numbers_len"] == 1 :
                tweet.at[row, "label"]="partial farm_rate"
            elif self.tweet_summary.label[0] == "farm_rate" and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("farm rate row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="farm_rate"

            if self.tweet_summary.label[0] == "doc_rate" and tweet.at[row, "chick"] >= 1 and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("doc rate row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="doc_rate"
           
            if self.tweet_summary.label[0] == "mandi_rate" and tweet.at[row, "rooster"] >= 1 and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 3:
                print("mandi rate row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="mandi_rate"
           
            if self.tweet_summary.label[0] == "supply_rate" and tweet.at[row, "rooster"] >= 1 and tweet.at[row,"words_supply_rate"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("supply rate row detected", tweet.at[row, "numbers"])
                tweet.at[row, "label"]="supply_rate"
            elif self.tweet_summary.label[0] == "supply_rate" and tweet.at[row, "egg"] >= 1 and tweet.at[row,"words_supply_rate"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("supply rate row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="partial_supply_rate"
            
            if self.tweet_summary.label[0] == "breeder_rate" and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("breeder rate row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="breeder_rate"
                            
            if self.tweet_summary.label[0] == "layer_culling_rate" and tweet.at[row,"words_layer_culling_rate"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("layer culling rate row detected", tweet.at[row, "numbers"])
                tweet.at[row, "label"]="partial_layer_culling_rate"

            if self.tweet_summary.label[0] == "egg_rate" and tweet.at[row, "egg"] >= 1 and tweet.at[row,"words_egg_rate"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("egg rate karachi row detected", tweet.at[row, "numbers"])
                tweet.at[row, "label"]="egg_rate_karachi"
            elif self.tweet_summary.label[0] == "egg_rate" and tweet.at[row,"words_egg_rate"] >= 1 and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] >= 1:
                print("egg rate punjab row detected", tweet.at[row, "numbers"])
                tweet.at[row, "label"]="egg_rate_punjab"
            elif self.tweet_summary.label[0] == "egg_rate" and tweet.at[row,"words_egg_rate"] >= 1 and tweet.at[row,"numbers_len"] >= 1:
                print("egg rate chakwal/sumundari row detected", tweet.at[row, "numbers"])
                tweet.at[row, "label"]="egg_rate_chakwal"

            if self.tweet_summary.label[0] == "supply_rate" and tweet.at[row, "rooster"] >= 1 and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("supply row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="supply_rate"
            elif self.tweet_summary.label[0] == "supply_rate" and tweet.at[row, "meat"] >= 1 and tweet.at[row,"cities_len"] >= 1 and tweet.at[row,"numbers_len"] == 1:
                print("supply row detected", tweet.at[row, "cities"], tweet.at[row, "numbers"])
                tweet.at[row, "label"]="supply_rate"



        if self.tweet_summary.at[0,"label"] == "doc_rate" :
            
            var_date=self.tweet_summary.date
            city_rate_df=tweet[(tweet.cities_len>0) & (tweet.numbers_len>0)]

            for index, row in city_rate_df.iterrows():
                if row["cities_len"]==1 and row["numbers_len"]==1:

                    data_model_obj=data_model()
                    data_model_obj.insert_doc_rate(
                        self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                        row["cities"][0],
                        row["numbers"][0],
                        int(self.tweet_summary.id[0])
                        )
            
        elif self.tweet_summary.at[0,"label"] == "farm_rate" :
            partial_farm_rate_df=tweet[tweet.label == "partial farm_rate"]["numbers"]
            partial_cityframe=tweet[tweet.cities_len > 0]["cities"]
            if len(partial_farm_rate_df)>0 and len(partial_cityframe)>0 :
                data_model_obj=data_model()
                data_model_obj.insert_farm_rate(
                    self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                    partial_cityframe.iloc[0][0],
                    partial_farm_rate_df.iloc[0][0],
                    int(self.tweet_summary.id[0])
                    )
                print("partial farm rate " , partial_cityframe.iloc[0],
                 partial_farm_rate_df.iloc[0], int(self.tweet_summary.id[0]))
            
            farm_rate_df=tweet[tweet.label == "farm_rate"]
            if len(farm_rate_df)>0 :
                for index, row in farm_rate_df.iterrows():
                    data_model_obj=data_model()
                    #print("cities ", farm_rate_df.at[index, "cities"])
                    #if(farm_rate_df.at[index, "cities_len"][0]>1)
                    for city in farm_rate_df.at[index, "cities"]:
                        data_model_obj.insert_farm_rate(
                            self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                            city,
                            farm_rate_df.at[index, "numbers"][0],
                            int(self.tweet_summary.id[0])
                            )


            
        elif self.tweet_summary.at[0,"label"] == "mandi_rate" :            
            mandi_rate_df=tweet[tweet.label == "mandi_rate"]
            if len(mandi_rate_df)>0 :
                for index, row in mandi_rate_df.iterrows():
                    data_model_obj=data_model()
                    data_model_obj.insert_mandi_rate(
                        self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                        mandi_rate_df.at[index, "cities"][0],
                        mandi_rate_df.at[index, "numbers"][0],
                        mandi_rate_df.at[index, "numbers"][1],
                        mandi_rate_df.at[index, "numbers"][2],
                        int(self.tweet_summary.id[0]))

        elif self.tweet_summary.at[0,"label"] == "breeder_rate" :            
            breeder_rate_df=tweet[tweet.label == "breeder_rate"]
            if len(breeder_rate_df)>0 :
                for index, row in breeder_rate_df.iterrows():
                    data_model_obj=data_model()
                    data_model_obj.insert_breeder_rate(
                        self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                        breeder_rate_df.at[index, "cities"][0],
                        breeder_rate_df.at[index, "numbers"][0],
                        int(self.tweet_summary.id[0]))
        elif self.tweet_summary.at[0,"label"] == "layer_culling_rate" :            
            layer_culling_rate_df=tweet[tweet.label == "partial_layer_culling_rate"]

            if len(layer_culling_rate_df)>0 :
                city_list=self.tweet_summary.cities[0].replace("' '","','")
                eval_city_list=eval(city_list)
                for city in eval_city_list:
                        data_model_obj=data_model()
                        data_model_obj.insert_layer_culling_rate(
                            self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                            city,
                            layer_culling_rate_df.iloc[0]["numbers"][0],
                            layer_culling_rate_df.iloc[1]["numbers"][0],
                            int(self.tweet_summary.id[0]))
        elif self.tweet_summary.at[0,"label"] == "egg_rate" :            
            partial_egg_rate_df=tweet[tweet.label == "egg_rate_chakwal"]

            if len(partial_egg_rate_df)>0 :
                city_list=self.tweet_summary.cities[0].replace("' '","','")
                eval_city_list=eval(city_list)
                
                print("cities ", self.tweet_summary.cities[0])
                print("city_list ", city_list)
                print("eval_city_list ", eval_city_list)
                print("partial_egg_rate_df ") 
                print(partial_egg_rate_df.iloc[0]["numbers"][0])
                print(partial_egg_rate_df.iloc[1]["numbers"][0])
                print(partial_egg_rate_df.iloc[2]["numbers"][1])

                for city in eval_city_list:
                        data_model_obj=data_model()
                        data_model_obj.insert_egg_rate(
                            self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                            city,
                            partial_egg_rate_df.iloc[0]["numbers"][0],
                            partial_egg_rate_df.iloc[1]["numbers"][0],
                            partial_egg_rate_df.iloc[2]["numbers"][1],
                            int(self.tweet_summary.id[0]))
            
            egg_rate_df=tweet[tweet.label == "egg_rate_karachi"]
            if len(egg_rate_df)>0 :
                for index, row in egg_rate_df.iterrows():
                    data_model_obj=data_model()
                    print("cities ", egg_rate_df.at[index, "cities"])
                    #if(farm_rate_df.at[index, "cities_len"][0]>1)
                    for city in egg_rate_df.at[index, "cities"]:
                        data_model_obj.insert_egg_rate(
                            self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                            city,
                            egg_rate_df.at[index, "numbers"][0],
                            "0",
                            "0",
                            int(self.tweet_summary.id[0])
                            )
            egg_rate_df=tweet[tweet.label == "egg_rate_punjab"]
            if len(egg_rate_df)>0 :
                for index, row in egg_rate_df.iterrows():
                    data_model_obj=data_model()
                    print("cities ", egg_rate_df.at[index, "cities"])
                    #if(farm_rate_df.at[index, "cities_len"][0]>1)
                    for city in egg_rate_df.at[index, "cities"]:
                        data_model_obj.insert_egg_rate(
                            self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                            city,
                            egg_rate_df.at[index, "numbers"][1],
                            "0",
                            "0",
                            int(self.tweet_summary.id[0])
                            )

        elif self.tweet_summary.at[0,"label"] == "supply_rate" :
            
            var_date=self.tweet_summary.date
            city_rate_df=tweet[(tweet.cities_len>0) & (tweet.numbers_len>0)]

            for index, row in city_rate_df.iterrows():
                if row["cities_len"]==1 and row["numbers_len"]==1:

                    data_model_obj=data_model()
                    data_model_obj.insert_supply_rate(
                        self.tweet_summary.date[0]+" "+self.tweet_summary.time[0],
                        row["cities"][0],
                        row["numbers"][0],
                        int(self.tweet_summary.id[0])
                        )
        return tweet

    def label_tweet(self):

        data_model_obj=data_model()
        tweet=data_model_obj.fetch_unlabel_tweet("1")

        print(len(tweet)," tweet need to be labeled")                
        if len(tweet)==0:
            return;

        tweet.drop(columns=['conversation_id', 'place', 'mentions', 'replies_count', 'retweets_count', 'likes_count', 'retweet', 'quote_url', 'near', 'geo', 'source', 'user_rt_id', 'user_rt', 'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src', 'trans_dest'], inplace=True)

        #all tweets processing
        #tweet=pd.DataFrame(index=range(len(single_tweet)))
        #tweets["orignal"]=single_tweet
        #tweets["tweet"]=single_tweet
        tweet["translate"]=""
        tweet["breeder_rate"]=0
        tweet["doc_rate"]=0
        tweet["egg_rate"]=0
        tweet["farm_rate"]=0
        tweet["last_year"]=0
        tweet["layer_culling_rate"]=0
        tweet["mandi_rate"]=0
        tweet["informational"]=0
        tweet["supply_rate"]=0
        tweet["obituary"]=0

        tweet["date_in_tweet"]=0
        tweet["cities"]=0
        tweet["cities"] = tweet["cities"].astype('object')
        tweet["cities_len"]=0
        tweet["numbers"]=0 
        tweet["numbers"] = tweet["numbers"].astype('object')
        tweet["numbers_len"]=0
        tweet["rooster"]=0
        tweet["egg"]=0
        tweet["chick"]=0
        tweet["meat"]=0
        tweet["words_breeder_rate"]=0
        tweet["words_doc_rate"]=0
        tweet["words_egg_rate"]=0
        tweet["words_farm_rate"]=0
        tweet["words_last_year"]=0
        tweet["words_layer_culling_rate"]=0
        tweet["words_mandi_rate"]=0
        tweet["words_obituary"]=0
        tweet["words_supply_rate"]=0
        tweet["junk"]=0
        tweet["label"]=""
        tweet["score"]=0
        
        #for i in range(len(tweet)):
        tweet=self.tweets_features_engineering(tweet, 0)
        

        tweet = self.assign_label(tweet)

        self.tweet_summary=tweet
        
        data_model_obj.update_label_table(tweet)

        tweet_array=tweet['tweet'].str.split('\n').explode()
        tweet=pd.DataFrame(tweet_array)
        tweet.index=range(len(tweet_array))

        #print(tweet_exploded)
        tweet["translate"]=""
        tweet["breeder_rate"]=0
        tweet["doc_rate"]=0
        tweet["egg_rate"]=0
        tweet["farm_rate"]=0
        tweet["last_year"]=0
        tweet["layer_culling_rate"]=0
        tweet["mandi_rate"]=0
        tweet["obituary"]=0
        tweet["supply_rate"]=0
        tweet["cities"]=0
        tweet["cities"] = tweet["cities"].astype('object')
        tweet["cities_len"]=0
        tweet["numbers"]=0 
        tweet["numbers"] = tweet["numbers"].astype('object')
        tweet["numbers_len"]=0
        tweet["rooster"]=0
        tweet["egg"]=0
        tweet["chick"]=0
        tweet["meat"]=0
        tweet["words_breeder_rate"]=0
        tweet["words_doc_rate"]=0
        tweet["words_egg_rate"]=0
        tweet["words_farm_rate"]=0
        tweet["words_last_year"]=0
        tweet["words_layer_culling_rate"]=0
        tweet["words_mandi_rate"]=0
        tweet["words_obituary"]=0
        tweet["words_supply_rate"]=0
        tweet["date_in_tweet"]=0
        tweet["junk"]=0
        tweet["label"]=""
        tweet["label_count"]=0

        for i in range(len(tweet)):
            tweet=self.tweets_features_engineering(tweet, i)
            
        self.tweet_exploded=tweet

        self.tweet_exploded=self.process_tweet_line(self.tweet_exploded)
