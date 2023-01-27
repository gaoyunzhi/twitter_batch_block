import twitter_batch_block
import plain_db

to_block = plain_db.load('to_block')

def test():
	for link, target in to_block.items.items():
		twitter_batch_block.block(link, target)

if __name__ == '__main__':
	test()