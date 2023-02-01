import twitter_batch_block
import plain_db
import time
from twitter_batch_block import *

to_block = plain_db.load('to_block')

def test():
	for link, target in to_block.items.items():
		twitter_batch_block.block(link, target)

def testSingleBlock():
	client = tweepy.Client(
        bearer_token=credential['bearer_token'],
        consumer_key=credential['consumer_key'],
        consumer_secret=credential['consumer_secret'],
        access_token=credential['access_key'],
        access_token_secret=credential['access_secret'])
	me = client.get_user(username=credential['main_user']).data
	me_followering = client.get_users_following(me.id).data
	target_user = client.get_user(username=credential['test_target_user']).data
	# print('me', me)
	# print('target_user', target_user)
	twitter_batch_block.singleBlock(client, target_user, me_followering)

if __name__ == '__main__':
	# test()
	# testSingleBlock()
	# load_db_all()
	load_db_additional()