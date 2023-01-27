from helper import kicklist, speaking_membership
from telegram_promote import S

def test():
	speaking_users = set()
	for user, item in speaking_membership.items():
		if str(S.settings['kick_target_group']) in item:
			speaking_users.add(user)
	print(len(speaking_users))
	for user in speaking_users:
		if not kicklist.contain(user):
			print(user)

if __name__ == '__main__':
	test()