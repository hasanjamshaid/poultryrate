import twint
import schedule
import time
import os
import datetime



os.environ['TWINT_DEBUG'] = 'debug'


# you can change the name of each "job" after "def" if you'd like.
def job_fetch_tweet_using_twint():
	time15minago=(datetime.datetime.now() - datetime.timedelta(minutes = 60))
	print("running job ", time15minago)
	current_time = time15minago.strftime(f"%Y-%m-%d %H:%M:%S")
	current_timestamp = time15minago.strftime(f"%Y%m%d%H%M%S")
	c = twint.Config()
	c.Username = "Shahzadsaeed240"
	c.Limit = 100
	c.Store_csv = True
	c.Output = "file"+current_timestamp+".csv"
	c.Since = current_time
	#c.Since = "2021-01-20 00:00:00"
	#c.Until = "2021-01-30 00:00:00"
	c.Show_hashtags = True
	c.Show_cashtags = True
	c.Stats = True
	c.Debug = True
	c.Pandas = True

	twint.run.Search(c)

def job_classify_tweet():
    print("job_classify_tweet")
    tweet_classifier_obj = tweet_classifier()
    tweet_classifier_obj.label_tweet()

def job_read_tweet_csv():
    print("job_read_tweets")
    csv_reader_obj=csv_reader()
    csv_reader_obj.read_tweet_csv_file("file*.csv")

def job_translate_tweets():
    print("job_translate_tweets")
    data_model_obj=data_model()
    data_model_obj.translate_unprocessed_tweet()



# run every minute(s), hour, day at, day of the week, day of the week and time. Use "#" to block out which ones you don't want to use.  Remove it to active. Also, replace "jobone" and "jobtwo" with your new function names (if applicable)

# schedule.every(5).minutes.do(jobone)
#schedule.every().hour.do(jobone)
# schedule.every().day.at("10:30").do(jobone)
# schedule.every().monday.do(jobone)
# schedule.every().wednesday.at("13:15").do(jobone)

schedule.every(0).minutes.do(job_fetch_tweet_using_twint)
schedule.every(0).minutes.do(job_read_tweet_csv)
schedule.every(0).minutes.do(job_classify_tweet)
schedule.every(0).minutes.do(job_translate_tweets)
#schedule.every().hour.do(jobtwo)
# schedule.every().day.at("10:30").do(jobtwo)
# schedule.every().monday.do(jobtwo)
# schedule.every().wednesday.at("13:15").do(jobtwo)

while True:
	schedule.run_pending()
	time.sleep(1)
