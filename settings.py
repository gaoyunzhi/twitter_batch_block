import plain_db
import yaml
import time
from helper import getPeerId
from telethon.tl.functions.channels import GetFullChannelRequest
from telegram_util import matchKey, isCN, isInt, getMatchedKey
import datetime

class Settings(object):
    def __init__(self):
        self.existing = plain_db.load('existing')
        self.message_loop = plain_db.load('message_loop')
        self.added_time = plain_db.load('added_time')
        self.my_messages = plain_db.loadKeyOnlyDB('my_messages')
        self.new_channels = plain_db.loadKeyOnlyDB('new_channels')
        self.existing_private_chat_user = plain_db.loadKeyOnlyDB('existing_private_chat_user')
        self._populateExisting()
        with open('credential') as f:
            self.credential = yaml.load(f, Loader=yaml.FullLoader)
        with open('settings.yaml') as f:
            self.settings = yaml.load(f, Loader=yaml.FullLoader)
        with open('groups.yaml') as f:
            self.groups = yaml.load(f, Loader=yaml.FullLoader)
        # testing
        # self.groups = {344758796: {}}
        self.all_subscriptions = self.settings.get('all_subscriptions')
        self.watching_keys = self.settings.get('watching_keys')
        self.mute_channel_id = self.settings.get('mute_channel_id')
        self.delete_all_channel_id = self.settings.get('delete_all_channel_id')
        self.check_id_channel_id = self.settings.get('check_id_channel_id')
        self.mute_keywords = self.settings.get('mute_keywords')
        self.default_client_name = self.settings.get('default_client_name')
        self.mark_as_read = self.settings.get('mark_as_read')
        self.daily_export_channels = self.settings.get('daily_export_channels')
        self.promote_user_ids = [item['id'] for item in self.credential['users'].values()]
        self.message_loop_hard_promote = self.settings.get('message_loop_hard_promote')
        self.history_limit = self.settings.get('history_limit')
        self.personal_channels = plain_db.loadKeyOnlyDB('personal_channels')

    def _populateExisting(self):
        self.group_log = {}
        self.message_log = {}
        for key, value in self.existing.items.items():
            target = key.split('=')[0]
            self.group_log[target] = max(self.group_log.get(target, 0), value)
            message = key[len(target) + 1:]
            self.message_log[message] = max(self.message_log.get(message, 0), value)

    def shouldSendToGroup(self, gid, setting):
        if setting.get('client_name') == 'zhou' and setting.get('username') and time.time() < 1632607939:
            return False
        if time.time() - self.group_log.get(str(gid), 0) < setting.get('gap_hour', 8) * 60 * 60:
            return False
        if not self.added_time.get(gid):
            self.added_time.update(gid, int(time.time()))
        return time.time() - self.added_time.get(gid) > 48 * 60 * 60 # 新加群不发言

    def isNoForwardMessage(self, message):
        if message.from_id and getPeerId(message.from_id) in self.promote_user_ids:
            self.my_messages.add('%d_%d' % (getPeerId(message.peer_id), message.id))
            return True
        return False

    def getPromoteMessage(self):
        loop_index = self.message_loop.get('message_loop_hard_promote', 0) % len(self.message_loop_hard_promote)
        return self.message_loop_hard_promote[loop_index]

    def shouldExclude(self, post):
        return matchKey(str(post), ['收到读者反馈', '关于评论区', 
            '多莉·帕顿', '我将大声疾呼女权主义重要性',
            '自己穿上假屌', '假屌是可拆卸', '翻译校对', '请求各位朋友', '我们都欢迎',
            '跨性别女性常常会将自己的阴茎塞入两腿之间', '请在涉事帖下方评论区留言',
            '翻译相关的事情', '请问有没有想要加入的朋友', '有兴趣加入我们',
            '请回复本消息', '我拉您进群', 'M5z558Z4', '推荐频道合集', 'https://t.me/channel_push',
            '欢迎加入我们的译者群', 'H2Be0QjdzJlJUMbP', '请帮我们转发这条招募译者的消息'])

    async def populateIdMap(self, client, subscription):
        channel = await client.get_entity(subscription)
        if self.settings['id_map'].get(subscription) == channel.id:
            return
        self.settings['id_map'][subscription] = channel.id
        self.save()

    async def getTier(self, message, client):
        if self.isFirstTier(message):
            return 0
        if self.isSecondTier(message):
            return 1
        is_third_tier = await self.isThirdTier(message, client)
        if is_third_tier:
            return 2
        return 3

    async def isThirdTier(self, message, client):
        if matchKey(str(message), [str(item) for item in self.mute_keywords]):
            return False
        if self.groups[getPeerId(message.peer_id)].get('tier_1'):
            return False # edit case
        if (self.groups[getPeerId(message.peer_id)].get('always_log') == 2 and 
            self.groups[getPeerId(message.peer_id)].get('tier_2')):
            return False # edit
        if message.edit_date and (not message.edit_hide) and self.groups[getPeerId(message.peer_id)].get('tier_2'):
            return False
        new_channel = await self.gotNewChannel(message, client)
        if new_channel:
            return True
        if not matchKey(str(message), self.watching_keys):
            return False
        if not self.groups[getPeerId(message.peer_id)].get('always_log'):
            return False
        if (message.fwd_from and message.fwd_from.channel_post) or message.media:
            return True
        if self.groups[getPeerId(message.peer_id)].get('log_media_only'):
            return False
        return len(message.raw_text) > 30

    def isFirstTier(self, message):
        return False
        # if message.edit_date:
        #     return False
        # return self.groups[getPeerId(message.peer_id)].get('tier_1')

    def forwardMyChannel(self, message):
        if message.fwd_from and message.fwd_from.channel_post:
            forward_from_id = getPeerId(message.fwd_from.from_id)
            if self.groups.get(forward_from_id, {}).get('log_forward'):
                return True
        return False

    # disable this check
    async def gotNewChannel(self, message, client):
        return False
        if 't.me/' not in str(message):
            return False
        if not isCN(str(message)): # 看看这个是否需要
            return False
        key = 'https://t.me/' + str(message).split('t.me/', 1)[1].split()[0].split("'")[0].split('"')[0].split('\\')[0].split('?')[0]
        if 'joinchat' not in key and isInt(key.split('/')[-1]):
            key = key.rsplit('/', 1)[0]
        if self.new_channels.contain(key):
            return False
        if '/c/' in key:
            return False
        try:
            group = await client.get_entity(key)
            group_full = await client(GetFullChannelRequest(channel=group))
            if group_full.full_chat.participants_count < 1000:
                return False
            if not isCN(str(group_full)):
                return False
            return self.new_channels.add(key)
        except Exception as e:
            return False

    def isSecondTier(self, message):
        if not matchKey(str(message), [str(item) for item in self.mute_keywords]):
            if self.forwardMyChannel(message):
                return True
        if message.edit_date and not message.edit_hide:
            return False
        if not self.groups[getPeerId(message.peer_id)].get('tier_2'):
            return False
        if self.groups[getPeerId(message.peer_id)].get('always_log') == 2:
            return True
        if not matchKey(str(message), self.watching_keys):
            return False
        if matchKey(str(message), [str(item) for item in self.mute_keywords]):
            return False
        if message.fwd_from and message.fwd_from.channel_post:
            return True
        if message.media:
            return True
        if 'source' in str(message):
            if len(getMatchedKey(str(message), self.watching_keys)) >= 2:
                return True
            if matchKey(str(message), ['doubanio.com', 'sinaimg.cn', 'f.video.weibocdn.com', 'telegra.ph']):
                return True
        return False

    def getAdditionalInfo(self, message):
        info = self.getMutedInfo(message)
        if not info:
            info = self.getKeyMatchInfo(message)
        if message.sticker:
            info += 'sticker '
        return info

    def getKeyMatchInfo(self, message):
        result = []
        for item in self.watching_keys:
            if str(item).lower() in str(message).lower():
                result.append(str(item))
        if not result:
            return ''
        return 'key_hit: ' + ' '.join(result) + ' '

    def getMutedInfo(self, message):
        result = []
        for item in self.mute_keywords:
            if str(item) in str(message):
                result.append(str(item))
        if not result:
            return ''
        if message.from_id and str(getPeerId(message.from_id)) == result[0] and len(result) == 1:
            return 'muted '
        if str(getPeerId(message.peer_id)) == result[0] and len(result) == 1:
            return 'group_muted '
        return 'muted: ' + ' '.join(result) + ' '

    def save(self):
        with open('settings.yaml', 'w') as f:
            f.write(yaml.dump(self.settings, sort_keys=True, indent=2, allow_unicode=True))