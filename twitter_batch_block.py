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

timer = {}
def wait(key, sec):
    time.sleep(max(0, timer.get(key, 0) - time.time()))
    timer[key] = time.time() + sec

def prepare_dbs():
    global followers_db, following_db
    followers_db = {}
    following_db = {}
    for user in credential['users'].items():
        followers_db = plain_db.loadKeyOnlyDB(user + '_followers')
        followering_db = plain_db.loadKeyOnlyDB(user + '_followering')

def get_client(user):
    return tweepy.Client(
        bearer_token=credential['bearer_token'],
        consumer_key=credential['consumer_key'],
        consumer_secret=credential['consumer_secret'],
        access_token=credential['users'][user]['access_key'],
        access_token_secret=credential['users'][user]['access_secret'])

def prepare_clients():
    global clients
    clients = {}
    for user in credential['users']:
        clients[user] = get_client(user)

def load_single(me, db_name, f):
    db = plain_db.loadKeyOnlyDB(db_name)
    wait(str(f), 60)
    result = f(me.id, None)
    while result.data:
        for user in result.data:
            db.add(user.username)
        token = result.meta.get('next_token')
        if not token:
            return
        wait(str(f), 65)
        result = f(me.id, token)
        
def load_dbs(user, client):
    suffix_to_method = {
        'followers': lambda username, pagination_token: client.get_users_followers(username, max_results=1000, pagination_token=pagination_token),
        'followering': lambda username, pagination_token: client.get_users_following(username, max_results=1000, pagination_token=pagination_token),
    }
    me = client.get_user(username=user).data
    for suffix, f in suffix_to_method.items():
        if user in credential['user_done']:
            continue
        load_single(me, user + '_' + suffix, f)

def load_db_all():
    prepare_clients()
    for user, client in clients.items():
        load_dbs(user, client)

def load_db_additional():
    main_user = credential['main_user']
    client = get_client(main_user)
    db = plain_db.loadKeyOnlyDB(main_user + '_followering')
    additionl_db = plain_db.loadLargeDB('additionl_db', isIntValue=False)
    for username in db.items():
        if additionl_db.get(username):
            continue
        wait('additionl_db', 65)
        user = client.get_user(username=username).data
        if not user:
            continue
        try:
            value = ' '.join([x.username for x in client.get_users_following(user.id, max_results=1000).data])
        except:
            continue
        additionl_db.update(username, value)

def get_intersection(list1, list2):
    set1 = set([x.username for x in list1])
    set2 = set([x.username for x in list2])
    print('set1', len(set1), set1)
    print('set2', len(set2), set2)
    return set1 & set2

def getLink(x):
    return 'https://twitter.com/' + x

def singleBlock(client, user, me_followering):
    followers = client.get_users_followers(user.id).data or []
    intersection = get_intersection(me_followering, followers)
    if intersection:
        print(' '.join([toLink(x) for x in user.username + list(intersection)]))
    else:
        ...
        # tele_target.send_message(user.username)

def block(link, target):
    tele_target = bot.get_chat(target)
    client = tweepy.Client(
        bearer_token=credential['bearer_token'],
        consumer_key=credential['consumer_key'],
        consumer_secret=credential['consumer_secret'],
        access_token=credential['access_key'],
        access_token_secret=credential['access_secret'])
    tweet_id = int(link.split('/')[-1])
    likers = client.get_liking_users(tweet_id).data or []
    retweeters = client.get_retweeters(tweet_id).data or []
    me = client.get_user(username=credential['main_user']).data
    me_followering = client.get_users_following(me.id).data
    for user in likers + retweeters:
        if existing.contain(user.username):
            continue
        time.sleep(120)
        singleBlock(client, user, me_followering)
        existing.add(user.username)
        