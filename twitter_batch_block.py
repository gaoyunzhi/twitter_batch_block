import yaml
import tweepy
from telegram.ext import Updater
import plain_db
import time

with open('token') as f:
    token = f.read().strip()
bot = Updater(token, use_context=True).bot 
with open('credential') as f:
    credential = yaml.load(f, Loader=yaml.FullLoader)
existing = plain_db.loadKeyOnlyDB('existing')

def get_intersection(list1, list2):
    set1 = set([x.username for x in list1])
    set2 = set([x.username for x in list2])
    return set1 & set2

def getLink(x):
    return 'https://twitter.com/' + x

def block(link, target):
    tele_target = bot.get_chat(target)
    clients = []
    for user, user_setting in credential['users'].items():
        client = tweepy.Client(
            bearer_token=credential['bearer_token'],
            consumer_key=credential['consumer_key'],
            consumer_secret=credential['consumer_secret'],
            access_token=user_setting['access_key'],
            access_token_secret=user_setting['access_secret'])
        if user == 'main':
            main_client = client
        else:
            clients.append(client)
    tweet_id = int(link.split('/')[-1])
    likers = main_client.get_liking_users(tweet_id).data
    retweeters = main_client.get_retweeters(tweet_id).data
    me = main_client.get_user(username=credential['main_user']).data
    main_followering = main_client.get_users_following(me.id).data
    for user in likers + retweeters:
        if existing.contain(user.username):
            continue
        time.sleep(1)
        followers = main_client.get_users_followers(user.id).data or []
        intersection = get_intersection(main_followering, followers)
        if intersection:
            print(' '.join([toLink(x) for x in user.username + list(intersection)]))
        else:
            tele_target.send_message(user.username)
        existing.add(user.username)