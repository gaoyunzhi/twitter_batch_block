import yaml
import tweepy
from telegram.ext import Updater
import plain_db
import time
import itertools

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
    global dbs
    dbs = {}
    for user in credential['users']:
        dbs[user + '_followers'] = plain_db.loadKeyOnlyDB(user + '_followers')
        dbs[user + '_followering'] = plain_db.loadKeyOnlyDB(user + '_followering')

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

def yieldFunc(f, sleep_key, time_sleep):
    wait(sleep_key, time_sleep)
    result = f(None)
    while result.data:
        for user in result.data:
            yield user
        token = result.meta.get('next_token')
        if not token:
            break
        wait(sleep_key, time_sleep)
        result = f(token)

def load_single(db_name, f, sleep_key):
    db = plain_db.loadKeyOnlyDB(db_name)
    for user in yieldFunc(f, sleep_key, 65):
        db.add(user.username)
        
def load_dbs(user, client):
    me = client.get_user(username=user).data
    suffix_to_method = {
        'followers': lambda pagination_token: client.get_users_followers(me.id, max_results=1000, pagination_token=pagination_token),
        'followering': lambda pagination_token: client.get_users_following(me.id, max_results=1000, pagination_token=pagination_token),
    }
    for suffix, f in suffix_to_method.items():
        if user in credential['user_done']:
            continue
        load_single(user + '_' + suffix, f, suffix)

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

def getLink(x):
    return 'https://twitter.com/' + x

def yieldIntersections(target_user, additionl_db):
    target = target_user.username
    for key, db in dbs.items():
        if target in db.items():
            yield key
    for user, items in additionl_db.items():
        if target in items.split():
            yield '%s_following' % user

def block(link, target):
    tele_target = bot.get_chat(target)
    debug_channel = bot.get_chat(credential['debug_channel_id'])

    prepare_dbs()
    additionl_db = plain_db.loadLargeDB('additionl_db', isIntValue=False)

    main_user = credential['main_user']
    client = get_client(main_user)
    tweet_id = int(link.split('/')[-1])
    generator1 = yieldFunc(lambda token: client.get_liking_users(tweet_id, pagination_token=token), 'get_liking_users', 12.2)
    generator2 = yieldFunc(lambda token: client.get_retweeters(tweet_id, pagination_token=token), 'get_retweeters', 12.2)

    count = 0
    for user in itertools.chain(generator1, generator2):
        if existing.contain(user.username):
            continue
        count += 1
        intersection = yieldIntersections(user, additionl_db)
        intersection = list(itertools.islice(intersection, 5))
        if intersection:
            message = '%s %s %s' % (getLink(user.username), user.username, ' '.join(intersection))
            debug_channel.send_message(message)
        else:
            wait(target, 5)
            tele_target.send_message(getLink(user.username))
        existing.add(user.username)
        if count == 10:
            break
        
        