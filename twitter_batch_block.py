import yaml
import tweepy

with open('credential') as f:
    credential = yaml.load(f, Loader=yaml.FullLoader)

def block(link, target):
    clients = []
    for user, user_setting in credential['users'].items():
         twitter_client = tweepy.Client(
            bearer_token=credential['bearer_token'],
            consumer_key=credential['consumer_key'],
            consumer_secret=credential['consumer_secret'],
            access_token=user_setting['access_key'],
            access_token_secret=user_setting['access_secret'])
        if user == main:
            main_client = twitter_client
        else:
            clients.append(twitter_client)
    tweet = main_client.get_status(int(link.split('/')[-1]), tweet_mode="extended")
    print(tweet)