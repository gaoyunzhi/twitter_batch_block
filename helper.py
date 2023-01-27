import yaml
import tweepy
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantsAdmins
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.functions.messages import ForwardMessagesRequest
from telethon.tl.functions.messages import GetHistoryRequest, SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty
from telegram_util import matchKey, isCN, isInt
import datetime
from telethon import types
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights, ChannelParticipantsSearch
from telethon.tl.functions.channels import GetParticipantsRequest
import plain_db
import time
import random
import json
from telegram_util import isUrl

namelist = plain_db.load('../../moderator/db/name', isIntValue=False)
kicklist = plain_db.loadKeyOnlyDB('../../moderator/db/kicklist')
mutelist = plain_db.loadKeyOnlyDB('../../moderator/db/mutelist')
group_membership = plain_db.loadLargeDB('../../msg_index_bot/db/group_membership')
speaking_membership = plain_db.loadLargeDB('../../msg_index_bot/db/speaking_membership')
group_name = plain_db.loadLargeDB('../../msg_index_bot/db/group_name')
group_link = plain_db.loadLargeDB('../../msg_index_bot/db/group_link')
name_history = plain_db.loadLargeDB('../../msg_index_bot/db/name_history')
name_history_plain = plain_db.loadKeyOnlyDB('../../msg_index_bot/db/name_history_plain')
translate_record = plain_db.loadLargeDB('translate_record')
translate_replied = plain_db.loadKeyOnlyDB('translate_replied')
user_channel = plain_db.loadKeyOnlyDB('user_channel')
stats_admin_set = plain_db.loadKeyOnlyDB('stats_admin_set')
moderate_admin_set = plain_db.loadKeyOnlyDB('moderate_admin_set')
added_to_others_index = plain_db.loadKeyOnlyDB('stats_admin_set')
log_channel_join_existing = plain_db.loadKeyOnlyDB('log_channel_join_existing')
translate_review = plain_db.loadKeyOnlyDB('translate_review')
record_history_existing = plain_db.loadKeyOnlyDB('record_history_existing')
group_names = plain_db.load('../backup/group_names', isIntValue=False)
no_mark_as_read = plain_db.loadKeyOnlyDB('no_mark_as_read')

DAY = 24 * 60 * 60
with open('twitter_credential') as f:
    twitter_credential = yaml.load(f, Loader=yaml.FullLoader)

last_send = {'time': 0}

def wait(second):
    w = last_send['time'] + second - time.time()
    if w > 0:
        time.sleep(w)
    last_send['time'] = time.time()

def getLinkFromId(group, message_id):
    try:
        if group.username:
            return 'https://t.me/%s/%d' % (group.username, message_id)
    except:
        ...
    return 'https://t.me/c/%s/%d' % (group.id, message_id)

def getChannelLink(entity):
    try:
        if entity.username:
            return 'https://t.me/%s' % entity.username
    except:
        ...
    return 'https://t.me/c/%s/1000000' % entity.id

def getLink(group, message):
    return getLinkFromId(group, message.id)

def getEntityInfo(entity):
    try:
        username = entity.username
    except:
        username = ''
    try:
        entity_info = [entity.id, entity.first_name, entity.last_name or '', '@' + username if username else '']
    except:
        entity_info = [entity.id, entity.title, '@' + username if username else '']
    entity_info = [str(item) for item in entity_info if item]
    return ' '.join(entity_info)

def getClient(clients, setting):
    client_name = setting.get('client_name') or next(iter(clients.keys()))
    return client_name, clients[client_name]

def getPostIds(target_post, posts):
    if target_post.grouped_id:
        for post in posts[::-1]:
            if post.grouped_id == target_post.grouped_id:
                yield post.id
    else:
        yield target_post.id

def getPeerId(peer_id):
    for method in [lambda x: x.channel_id, 
        lambda x: x.chat_id, lambda x: x.user_id]:
        try:
            return method(peer_id)
        except:
            ...

def noAutoMod(group):
    try:
        group.entity.participants_count
        group.entity.title
    except:
        return True
    if not group.entity.participants_count or group.entity.participants_count < 15:
        return True
    if matchKey(group.entity.title, ['辟谣', '闢謠', '性别议题群']):
        return True
    if hasUsername(group.entity) and matchKey(group.entity.username, ['fengsuo']):
        return True
    if group.entity.id in [1590314399]: 
        return True
    if not group.entity.admin_rights:
        return True
    return False

async def unpinTranslated(client):
    chat = await client.get_entity(1386450222)
    messages = await client.get_messages(chat, filter=types.InputMessagesFilterPinned(), limit=500)
    for message in messages:
        if not message.raw_text:
            continue
        if matchKey(message.raw_text, ['已完成', '已翻译']):
            try:
                await client.unpin_message(chat, message.id)
            except Exception as e:
                print(e)
                return

async def logChannelJoin(client):
    log_group = await client.get_entity(1362160328)
    check_group = await client.get_entity(1743869157)
    for channel_id in [1668853940, 1177823847, 1155581129]:
        channel = await client.get_entity(channel_id)
        admin_logs = await client.get_admin_log(channel, join=True)
        for log in admin_logs:
            user_id = log.user_id
            user = await client.get_entity(user_id)
            key = '%d_%d' % (channel.id, user_id)
            if log_channel_join_existing.contain(key):
                continue
            log_line = '%s joined [%s](%s)' % (getEntityInfo(user), channel.title, getChannelLink(channel))
            await client.send_message(log_group, log_line, link_preview=False)
            await client.send_message(check_group, str(user_id))
            log_channel_join_existing.add(key)

async def replyTranslated(client):
    translated = set()
    for chat_id in [1742163696, 1240049600]:
        chat = await client.get_entity(chat_id)
        messages = await client.get_messages(chat, limit=100)
        for message in messages:
            text = message.raw_text or message.text or ''
            for i in range(len(text) - 9):
                snippet = text[i:i+10]
                if sum([isCN(x) for x in snippet]) > 5:
                    translated.add(text[i:i+10])
    chat = await client.get_entity(1347960785)
    messages = await client.get_messages(chat, limit=200)
    for message in messages:
        if message.fwd_from:
            continue
        text = message.raw_text or message.text or ''
        if not text:
            continue
        text = ''.join(text.split())
        translate_record.update(message.id, text)
    for message_id, text in translate_record.items():
        if translate_replied.contain(message_id):
            continue
        if message_id == 43381:
            print(text)
            print(translated)
        if not matchKey(text, translated):
            continue
        translate_replied.add(message_id)
        message = await client.get_messages(chat, ids=int(message_id))
        await message.reply('感谢！已发布~~')

async def checkMemberHistory(client):
    channel = await client.get_entity(1603460097)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        if not message.raw_text:
            continue
        if message.raw_text.startswith('done'):
            continue
        result = 'done: ' + message.raw_text + '\n'
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            if message.raw_text.strip() in str(dialog.entity):
                break
        if message.raw_text.strip() not in str(dialog.entity):
            await client.edit_message(
                channel,
                message.id,
                text = result + 'group not found',
                parse_mode='Markdown',
                link_preview=False)
            continue
        participants = await client(GetParticipantsRequest(
            dialog.entity, ChannelParticipantsSearch(''), 0, 100, 0
        ))
        for user in participants.users:
            if kicklist.contain(user.id):
                result +='\n%d in kicklist %s' % (user.id, namelist.get(user.id, '[%s](tg://user?id=%d)' % (user.first_name, user.id)))
            if mutelist.contain(user.id):
                result +='\n%d in mutelist %s' % (user.id, namelist.get(user.id, '[%s](tg://user?id=%d)' % (user.first_name, user.id)))
        await client.edit_message(
            channel,
            message.id,
            text = result,
            parse_mode='Markdown',
            link_preview=False)

def getUserName(user):
    username = user.first_name
    if user.last_name: 
        username += ' ' + user.last_name
    if user.username:
        username += ' @' + user.username
    return username

def updateNameHistory(user_id, username):
    if name_history_plain.contain('%d_%s' % (user_id, username)):
        return
    history = name_history.get(user_id) or '[]'
    history = yaml.safe_load(history)
    for item in history:
        if item.split('_', 1)[1] == username:
            return
    history.append('%d_%s' % (time.time(), username))
    name_history.update(user_id, json.dumps(history))
    name_history_plain.add('%d_%s' % (user_id, username))

def updateGroupMembership(user_id, group_id, membership_file = group_membership):
    history = membership_file.get(user_id) or ''
    for item in history.split():
        _, gid = item.split('_')
        if gid == str(group_id):
            return
    history += ' %d_%d' % (time.time(), group_id)
    history = history.strip()
    membership_file.update(user_id, history)

def updateGroupName(dialog):
    group_name.update(dialog.entity.id, dialog.title)
    group_link.update(dialog.entity.id, getChannelLink(dialog.entity))

async def getParticipantsWithLimit(client, entity, limit):
    return await client.get_participants(entity, limit=limit)

async def tryGetParticipants(client, entity):
    try:
        return await client.get_participants(entity)
    except:
        ...
    for limit in [10000, 5000, 2000, 1000, 500]:
        try:
            result = await getParticipantsWithLimit(client, entity, limit)
            # print('get_participants success', limit, entity.title)
            return result
        except Exception as e:
            # print('get_participants failed', limit, entity.title, e)
            ...

async def tryUpdateSpeakingParticipants(client, entity):
    count = 0
    async for message in client.iter_messages(entity):
        count += 1
        if count > 1000:
            break 
        if not message.from_id:
            continue
        try:
            user_id = message.from_id.user_id
        except:
            continue
        if message.action:
            updateGroupMembership(user_id, entity.id, group_membership)
        else:
            updateGroupMembership(user_id, entity.id, speaking_membership)

async def getNewAdditionalGroup(client):
    channel = await client.get_entity(1876726496)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    new_additional_groups = set()
    for message in group_posts.messages:
        if not message.raw_text:
            continue
        if message.raw_text.startswith('done'):
            continue
        result = 'done: ' + message.raw_text + '\n'
        try:
            new_additional_groups.add(message.raw_text.split('/')[-2])
        except:
            continue
        await client.edit_message(
            channel,
            message.id,
            text = result,
            parse_mode='Markdown',
            link_preview=False)
    return new_additional_groups

async def logUserChannel(clients, S):
    count = 0
    client_names = list(clients.keys())
    client_names.remove('zhou')
    client_names = ['zhou'] + client_names
    clients = [clients[name] for name in client_names]
    additonal_get = False
    for client in clients:
        dialogs = await client.get_dialogs()
        random.shuffle(dialogs)
        new_dialogs = [dialog for dialog in dialogs if not group_name.get(dialog.entity.id)]
        for dialog in new_dialogs + dialogs:
            try:
                dialog.entity.participants_count
            except Exception as e:
                continue
            if dialog.is_channel and not dialog.entity.megagroup and not dialog.entity.admin_rights:
                continue
            if not dialog.is_channel or dialog.entity.megagroup:
                await tryUpdateSpeakingParticipants(client, dialog.entity)
            users = await tryGetParticipants(client, dialog.entity)
            if not users:
                continue
            to_break = False
            if group_name.get(dialog.entity.id):
                to_break = True
            updateGroupName(dialog)
            for user in users:
                username = getUserName(user)
                updateNameHistory(user.id, username)
                updateGroupMembership(user.id, dialog.entity.id)
            if to_break:
                break
        additonal_group = list(S.settings.get('log_additional_group'))
        random.shuffle(additonal_group)
        if not additonal_get:
            additonal_get = True
            new_groups = await getNewAdditionalGroup(clients[1])
            needSave = False
            if new_groups:
                needSave = True
                S.settings['log_additional_group'] += list(new_groups)
            for gid in additonal_group:
                try:
                    group = await client.get_entity(gid)
                except Exception as e:
                    continue
                if gid != group.id:
                    S.settings['log_additional_group'].remove(gid)
                    S.settings['log_additional_group'].append(group.id)
                    needSave = True
                group_name.update(group.id, group.title)
                group_link.update(group.id, getChannelLink(group))
                await tryUpdateSpeakingParticipants(client, group)
            if needSave:
                S.settings['log_additional_group'] = list(set(S.settings['log_additional_group']))
                S.save()

async def checkUserChannel(client):
    channel = await client.get_entity(1618113434)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        if not message.raw_text:
            continue
        if message.raw_text.startswith('done'):
            break
        result = 'done: ' + message.raw_text
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            try:
                dialog.entity.participants_count
            except:
                continue
            if dialog.is_group:
                continue
            try:
                participants = await client(GetParticipantsRequest(
                    dialog.entity, ChannelParticipantsSearch(message.raw_text), 0, 100, 0
                ))
                if participants.users:
                    result += ' [%s](%s)' % (dialog.title, getChannelLink(dialog.entity))
            except:
                ...
        await client.edit_message(
            channel,
            message.id,
            text = result,
            parse_mode='Markdown',
            link_preview=False)

async def deleteSingle(client, message):
    entity = await client.get_entity(getPeerId(message.peer_id))
    forward_group = await client.get_entity(1223777401)
    message_link = getLink(entity, message)
    if not message.grouped_id:
        try:
            await client.forward_messages(forward_group, message.id, entity)
            await client.send_message(forward_group, message_link)
            await client.delete_messages(entity, message.id)
            return 1
        except Exception as e:
            # print('delete failed', str(e), message_link)
            return 0
    messages = await client.get_messages(entity, min_id = message.id, max_id = message.id + 10)
    result = [message]
    for post in messages:
        if post.grouped_id and post.grouped_id == message.grouped_id:
            result.append(post)
    final_result = 0
    for post in result:
        try:
            await client.forward_messages(forward_group, post.id, entity)
            await client.delete_messages(entity, post.id)
            final_result += 1
        except Exception as e:
            # print('delete failed', str(e), message_link)
            ...
    await client.send_message(forward_group, message_link)
    return final_result

def getDisplayLink(group, message, groups):
    invitation_link = groups.get(group.id, {}).get('invitation_link')
    suffix = ''
    if message.reply_to and message.reply_to.reply_to_msg_id:
        suffix += ' [主贴](%s)' % getLinkFromId(group, message.reply_to.reply_to_msg_id)
    if invitation_link:
        suffix += ' [进群](%s)' % invitation_link
    try:
        title = group.title
    except:
        title = '%s %s' % (group.first_name, group.last_name or '')
        if group.username:
            title += ' @' + group.username
    return '[%s](%s)%s' % (title, getLink(group, message), suffix)

async def addChannelSingle(clients, text, S):
    client_names = list(clients.keys())
    client_names.remove('yun')
    client_names.append('yun')
    group = None
    try:
        text = int(text)
    except:
        ...
    group = None
    for client_name in client_names:
        dialogs = await clients[client_name].get_dialogs()
        for dialog in dialogs:
            if dialog.entity.id == text:
                group = dialog.entity
                break
            if str(text) in str(dialog.entity) and dialog.entity.id != 1475165266:
                group = dialog.entity
                break
        if group:
            break
    if not group:
        return 'group not find'
    if group.id in S.groups:
        return 'group exists ' + str(group.id)
    setting = {'client_name': client_name, 'promoting': 0, 'kicked': 0, 'newly_added': 1}
    try:
        if group.username:
            setting['username'] = group.username
    except:
        ...
    if 'joinchat' in str(text):
        setting['invitation_link'] = text
    setting['title'] = group.title
    S.groups[group.id] = setting
    with open('groups.yaml', 'w') as f:
        f.write(yaml.dump(S.groups, sort_keys=True, indent=2, allow_unicode=True)) 
    return 'success'

async def addChannel(clients, S):
    client = clients['yun']
    channel = await client.get_entity(1475165266)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    count = 0
    for message in group_posts.messages:
        if not message.raw_text or message.raw_text.startswith('done'):
            continue
        result = await addChannelSingle(clients, message.raw_text, S)
        await client.edit_message(
            channel,
            message.id,
            text = 'done %s: %s' % (message.raw_text, result))

async def twitterBlockPerChannel(clients, channel_id):
    client = clients['yun']
    channel = await client.get_entity(channel_id)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=200,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        if not message.raw_text or message.raw_text.startswith('done'):
            continue
        screen_name = message.raw_text.strip().split()[0].split('/')[-1]
        additional_info = ''
        try:
            for _, twitter_user_setting in twitter_credential['users'].items():
                auth = tweepy.OAuthHandler(twitter_credential['consumer_key'], twitter_credential['consumer_secret'])
                auth.set_access_token(twitter_user_setting['access_key'], twitter_user_setting['access_secret'])
                api = tweepy.API(auth)
                result = api.create_block(screen_name=screen_name)
        except Exception as e:
            additional_info = str(e)
        await client.edit_message(
            channel,
            message.id,
            text = 'done https://twitter.com/%s %s' % (screen_name, additional_info))
        sub_channel = await clients['yun'].get_entity(1297352950)
        await clients['yun'].send_message(sub_channel, '/b %s' % screen_name)
        return True # only block one account at one time

async def twitterBlock(clients):
    result = await twitterBlockPerChannel(clients, 1581263596)
    if result:
        return
    result = await twitterBlockPerChannel(clients, 1628388704)

async def twitterHideReply(client):
    channel = await client.get_entity(1717826288)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=200,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        if not message.raw_text or message.raw_text != 'h':
            continue
        if not message.reply_to_msg_id:
            continue
        origin_message = await client.get_messages(channel, ids=message.reply_to_msg_id)
        raw_text = origin_message.raw_text
        if not raw_text:
            continue
        for twitter_at in raw_text.split():
            twitter_account = twitter_at[1:]
            twitter_user_setting = twitter_credential['users'].get(twitter_account)
            if twitter_user_setting:
                break
        if not twitter_user_setting:
            continue
        twitter_client = tweepy.Client(
            bearer_token=twitter_credential['bearer_token'],
            consumer_key=twitter_credential['consumer_key'],
            consumer_secret=twitter_credential['consumer_secret'],
            access_token=twitter_user_setting['access_key'],
            access_token_secret=twitter_user_setting['access_secret'])
        twitter_url = origin_message.entities[-1].url
        try:
            twitter_client.hide_reply(int(twitter_url.split('/')[-1]))
            await client.edit_message(
                channel,
                message.id,
                text = 'hidden')
        except:
            await client.edit_message(
                channel,
                message.id,
                text = 'hide fail')

def hasUsername(entity):
    try:
        return entity.username
    except:
        return False

def hasTitle(entity):
    try:
        return entity.title
    except:
        return False
   
async def setModerateAdmin(clients):
    for client_name, client in clients.items():
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            if moderate_admin_set.contain(client_name + str(dialog.id)):
                continue
            try:
                dialog.entity.participants_count
            except Exception as e:
                continue
            if not hasUsername(dialog.entity) and dialog.entity.participants_count < 10:
                continue
            if not dialog.entity.admin_rights:
                continue
            try:
                for user_id in [909398533]:
                    exist = False
                    async for user in client.iter_participants(dialog.entity, filter=ChannelParticipantsAdmins):
                        if user.id == user_id:
                            exist = True
                    if exist:
                        continue
                    user = await client.get_entity(user_id)
                    await client.edit_admin(dialog.entity, user, post_messages=False, ban_users=True, add_admins=True)
                print('set moderate admin success / existed', client_name, dialog.name, exist)
            except Exception as e:
                print('set moderate admin fail', client_name, dialog.name, e)
            moderate_admin_set.add(client_name + str(dialog.id))
        
async def setStatsAdmin(clients):
    for client_name, client in clients.items():
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            if stats_admin_set.contain(client_name + str(dialog.id)):
                continue
            try:
                dialog.entity.participants_count
            except Exception as e:
                continue
            if not hasUsername(dialog.entity) and dialog.entity.participants_count < 50:
                continue
            if not dialog.entity.admin_rights:
                continue
            try:
                for user_id in [1061392438, 433791261, 210944655]:
                    user = await client.get_entity(user_id)
                    await client.edit_admin(dialog.entity, user, add_admins=True)
                print('set admin success', client_name, dialog.name)
            except Exception as e:
                print('set admin fail', client_name, dialog.name, e)
            stats_admin_set.add(client_name + str(dialog.id))
            break


async def addToOthersIndex(clients):
    dialogs = await clients['yun'].get_dialogs()
    for dialog in dialogs:
        if added_to_others_index.contain(dialog.id):
            continue
        try:
            dialog.entity.participants_count
        except Exception as e:
            continue
        if not hasUsername(dialog.entity):
            continue
        if not dialog.entity.admin_rights:
            continue
        user = await clients['zhou'].get_entity(401234709)
        await clients['zhou'].send_message(user, '/add @' + dialog.entity.username)
        added_to_others_index.add(dialog.id)
        break

async def kickPersonalChannels(clients, S):
    client = clients['yun']
    channel = await client.get_entity(1727142341)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        if message.raw_text and message.raw_text.startswith('done'):
            continue
        try:
            text = message.raw_text.split()[0]
            if isInt(text):
                user_id = int(text)
            else:
                user_id = text.strip()
            user = await client.get_entity(user_id)
        except Exception as e:
            continue
        actioned_set = set()
        for client_name in ['zhou', 'yun']:
            sub_client = clients[client_name]
            try:
                user = await sub_client.get_entity(user_id)
            except:
                continue
            groups = await sub_client.get_dialogs()
            count = 0
            for group in groups:
                if group.entity.id in actioned_set:
                    continue
                if not S.personal_channels.contain(str(group.entity.id)):
                    continue
                if not group.entity.admin_rights:
                    continue
                try:
                    result = await sub_client(EditBannedRequest(
                        group, user, ChatBannedRights(until_date=None, view_messages=True)))
                    actioned_set.add(group.entity.id)
                    count += 1
                except Exception as e:
                    ...
        await client.edit_message(
            channel,
            message.id,
            text = 'done %s: kicked %d times' % (getEntityInfo(user), len(actioned_set)))

def isPriorityKickGroup(group):
    return group.entity.id in [1863117184, 1588033090]
    # return group.entity.id in [1164942987] 

async def kickAllInculdingChannels(clients, S, 
        main_channel_id=1589897379, chat_rights = ChatBannedRights(until_date=None, view_messages=True),
        action_text = 'kicked'):
    client = clients['yun']
    channel = await client.get_entity(main_channel_id)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=200,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    last_run = None
    for message in group_posts.messages:
        if message.raw_text and message.raw_text.startswith('done'):
            if last_run and (message.edit_date or message.date):
                last_run = max(last_run, message.edit_date or message.date)
            else:
                last_run = message.edit_date or message.date
    dry_run = False
    if last_run and time.time() - datetime.datetime.timestamp(last_run) < 180 * 60:
        dry_run = True
    for message in group_posts.messages:
        if not message.raw_text or message.raw_text.startswith('done'):
            continue  
        if matchKey(message.raw_text, ['force']):
            dry_run = False
        if dry_run and matchKey(message.raw_text, ['times for now']):
            continue
        try:
            text = message.raw_text.split()[0]
            if isInt(text):
                user_id = int(text)
            else:
                user_id = text.strip()
            user = await client.get_entity(user_id)
        except Exception as e:
            if not dry_run:
                continue
        actioned_set = set()
        for client_name in ['zhou', 'yun']:
            sub_client = clients[client_name]
            try:
                user = await sub_client.get_entity(user_id)
            except:
                continue
            groups = await sub_client.get_dialogs()
            groups = ([group for group in groups if isPriorityKickGroup(group)] + 
                [group for group in groups if not isPriorityKickGroup(group)])
            # see if I need to reorder the group so some group get first and always 
            # start kick in dry run mode
            count = 0
            for group in groups:
                if group.entity.id in actioned_set:
                    continue
                if noAutoMod(group):
                    continue
                # print('kick from channel/group:', group.entity.title)
                try:
                    result = await sub_client(EditBannedRequest(
                        group, user, chat_rights))
                    actioned_set.add(group.entity.id)
                    count += 1
                except:
                    ...
                if dry_run and count > 5:
                    break
            # print(client_name, len(actioned_set))
        if dry_run:
            await client.edit_message(
                channel,
                message.id,
                text = '%s: %s %d times for now' % (getEntityInfo(user), action_text, len(actioned_set)))
            continue
        await client.edit_message(
            channel,
            message.id,
            text = 'done %s: %s %d times' % (getEntityInfo(user), action_text, len(actioned_set)))
        return # only kick one at a time


async def unkickAllInculdingChannels(clients, S): 
    await kickAllInculdingChannels(clients, S, main_channel_id=1621077636,
        chat_rights=ChatBannedRights(until_date=None, view_messages=False), action_text='unkicked')

async def getRelatedPosts(client, chat, message):
    if not message.grouped_id:
        return [message]
    posts = await client.get_messages(chat, min_id=message.id - 1, max_id = message.id + 10)
    return [post for post in posts if post.grouped_id == message.grouped_id]

def getLinkInText(text):
    for link in text.split()[::-1]:
        if isUrl(link):
            return ' [source](%s)' % link
    return ''

async def getSig(client, user_id):
    try:
        user = await client.get_entity(user_id)
    except:
        return ''
    text = user.last_name
    if text and '不署名' in text:
        return ''
    if text and '署名' in text:
        sig = text.split('署名')[-1].strip()
        if sig:
            return '\n\n译者： ' + sig
    return ''

async def tryMarkAsRead(client, entity):
    try:
        messages = await client.get_messages(entity, limit=1)
        await client.send_read_acknowledge(entity, messages[0])
    except:
        ...

async def markAsRead(clients, S):
    dialogs = await clients['zhou'].get_dialogs()
    for dialog in dialogs:
        if dialog.unread_count and type(dialog.entity).__name__ == 'User':
            await tryMarkAsRead(clients['zhou'], dialog.entity)

    dialogs = await clients['yun'].get_dialogs()
    for dialog in dialogs:
        if dialog.unread_count:
            if type(dialog.entity).__name__ == 'User' and not dialog.entity.bot:
                continue
            if (no_mark_as_read.contain(dialog.entity.id) or 
                (hasUsername(dialog.entity) and no_mark_as_read.contain(dialog.entity.username)) or 
                (hasTitle(dialog.entity) and no_mark_as_read.contain(dialog.entity.title))):
                continue
            await tryMarkAsRead(clients['yun'], dialog.entity)

async def recordHistory(client, S):
    channel_daily = await client.get_entity(1703789500)
    channel_full = await client.get_entity(1850658278)
    channel_record = await client.get_entity(1362160328)
    for group_id, setting_detail in S.daily_export_channels.items():
        group = await client.get_entity(group_id)
        group_title = group_names.get(group_id)
        if not group_title: 
            group_title = group.title
        daily_filename = 'backup/%s_daily.txt' % group_title
        daily_filename_lastpart = '%s_daily.txt' % group_title
        full_filename = 'backup/%s.txt' % group_title
        messages = await client.get_messages(group, limit=200)
        with open(daily_filename, 'w') as f:
            f.write('\n')
        updated = False
        for message in messages[::-1]:
            if setting_detail.get('only_fetch_certai_topic'):
                try:
                    if message.reply_to.reply_to_msg_id != setting_detail.get('only_fetch_certai_topic'):
                        continue
                except:
                    continue
            from_user_id = getPeerId(message.from_id)
            log_line = '%s %s\n' % (from_user_id, message.raw_text or message.text)
            key = '%d=%d' % (group_id, message.id)
            if not record_history_existing.contain(key):                
                with open(full_filename, 'a') as f:
                    f.write(log_line)
                if from_user_id and (not matchKey(from_user_id, S.mute_keywords)) and (message.raw_text or message.text):
                    # await client.send_message(channel_record, log_line)
                    ... 
                record_history_existing.add(key)
                updated = True
            if int((datetime.datetime.now(datetime.timezone.utc) - message.date)
                .total_seconds()) < DAY:
                with open(daily_filename, 'a') as f:
                    f.write(log_line)
        if updated:
            await client.send_file(channel_full, full_filename)
            if setting_detail.get('no_daily'):
                continue
            messages = await client.get_messages(channel_daily, limit=10)
            for message in messages:
                if daily_filename_lastpart in str(message):
                    await message.delete()
            await client.send_file(channel_daily, daily_filename)

async def translateReview(clients, client_name, S):
    client = clients[client_name]
    chat = await client.get_entity(1347960785)
    messages = await client.get_messages(chat, limit=200)
    forward_group = await client.get_entity(1546520023)
    for message in messages:
        if translate_review.contain(message.id):
            continue
        if message.fwd_from:
            continue
        text = message.raw_text or message.text or ''
        if not text:
            continue
        if len(text) < 10:
            continue
        if not message.reply_to:
            continue
        if getPeerId(message.from_id) == S.credential['users'][client_name]['id']:
            continue
        reply_to_message = await client.get_messages(chat, ids=message.reply_to.reply_to_msg_id)
        if not reply_to_message:
            continue
        reply_to_text = reply_to_message.raw_text or reply_to_message.text or ''
        if not reply_to_message.grouped_id and isUrl(reply_to_text):
            continue
        if getPeerId(reply_to_message.from_id) != 1386450222:
            continue
        posts = await getRelatedPosts(client, chat, reply_to_message)
        result = await client(ForwardMessagesRequest(
            from_peer=chat,
            id=[post.id for post in posts],
            to_peer=forward_group,
            drop_author=True
        ))
        sig = await getSig(client, getPeerId(message.from_id))
        try:
            await client.edit_message(
                forward_group,
                result.updates[0].id,
                text = text.strip() + getLinkInText(reply_to_text) + sig)
        except:
            suffix_len = 7 if getLinkInText(reply_to_text) else 0
            suffix_len += len(sig)
            await client.edit_message(
                forward_group,
                result.updates[0].id,
                text = text.strip()[:1024 - suffix_len] + getLinkInText(reply_to_text) + sig)
        translate_review.add(message.id)

async def deleteTarget(client, target):
    if len(target) < 3:
        return 0
    if (not isCN(target)) and len(target) < 5:
        return 0
    dialogs = await client.get_dialogs()
    result = []
    for dialog in dialogs:
        if type(dialog.entity).__name__ == 'User':
            continue
        try:
            if dialog.entity.participants_count < 100:
                continue
        except:
            print(dialog)
            continue
        if noAutoMod(dialog):
            continue
        if dialog.entity.id in [1164942987]: 
            continue
        messages = await client.get_messages(entity=dialog.entity, search=target, limit=50)
        messages = [message for message in messages if target in message.text]
        result += messages
    result = [message for message in result if target in message.text]    
    result = [message for message in result if not matchKey(message.text, ['【保留】', '【不删】'])]
    if len(result) > 200:
        print('too many matches for delete: %s, %d', target, len(result))
        return 0
    final_result = 0
    for message in result:
        final_result += await deleteSingle(client, message)
    return final_result

async def deleteOldForGroup(client, group, dry_run = False, hour_cut = 20):
    user = await client.get_me()
    result = await client(SearchRequest(
        peer=group,     # On which chat/conversation
        q='',           # What to search for
        filter=InputMessagesFilterEmpty(),  # Filter to use (maybe filter for media)
        min_date=None,  # Minimum date
        max_date=None,  # Maximum date
        offset_id=0,    # ID of the message to use as offset
        add_offset=0,   # Additional offset
        limit=1000,       # How many results
        max_id=0,       # Maximum message ID
        min_id=0,       # Minimum message ID
        from_id=user,
        hash=0
    ))
    max_id = None
    count = 0
    for message in result.messages:
        if not max_id:
            max_id = message.id
            continue
        if int((datetime.datetime.now(datetime.timezone.utc) - message.date)
            .total_seconds()) < 60 * 60 * hour_cut:
            continue
        if max_id - message.id < 50:
            continue
        if not message.from_id or getPeerId(message.from_id) != user.id:
            continue
        if dry_run:
            count += 1
        else:
            result = await deleteSingle(client, message)
            count += result
    return count

async def deleteOld(client_map, S):    
    count = 0 
    # 暂时不用，删除所有旧历史
    for client_name, client in client_map.items():
        groups = await client.get_dialogs()
        for group in groups:
            if group.name not in ['Backlight.Town 逆光小镇', '独自', '在花の科技花 🎗 元宇宙', 
                '平权观察', 'Yonezu KenShu 抱抱群', '吕频']:
                continue
            result = await deleteOldForGroup(client, group.entity)
            if result > 0:
                print('deleted %s messages in %s' % (result, group.name))
            count += result
    # 这段是删除特定promote群组旧历史
    # for gid in S.groups:
    #     for client_name, client in client_map.items():
    #         try:
    #             group = await client.get_entity(gid)
    #         except:
    #             continue
    #         if not group.megagroup:
    #             continue
    #         result = await deleteOldForGroup(client, group)
    #         count += result
    if count != 0:
        print('deleted old message:', count)

async def checkUserID(client_map, S, C):
    client = client_map['yun']
    channel = await client.get_entity(S.check_id_channel_id)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        text = message.raw_text
        if not text:
            continue
        if not text.startswith('https://t.me'):
            continue
        if len(text.split()) > 1:
            continue
        target_message_id = int(text.split('/')[-1].split('?thread=')[0])
        if len(text.split('/')) == 5:
            target_channel_key = '/'.join(text.split('/')[:-1])
            for _, tmp_client in client_map.items():
                try:
                    await C.getPosts(tmp_client, target_channel_key, S) # to populate id map
                    target_channel = await C.getChannel(tmp_client, target_channel_key, S)
                    break
                except:
                    ...
        else:
            target_channel_id = int(text.split('/')[-2])
            for _, tmp_client in client_map.items():
                try:
                    target_channel = await tmp_client.get_entity(target_channel_id)
                    break
                except:
                    ...
        # print(target_channel.id, target_message_id)
        target_message = await tmp_client.get_messages(target_channel, ids=target_message_id)
        if target_message:
            user_id = getPeerId(target_message.from_id)
        else:
            user_id = 0
        await client.edit_message(
            channel,
            message.id,
            text = 'done: %s user_id: %d' % (text, user_id))

async def deleteAll(client_map, S):
    client_names = list(client_map.keys())
    client_names.remove('yun')
    client_names = ['yun'] + client_names
    clients = [client_map[name] for name in client_names]
    client = clients[0]
    channel = await client.get_entity(S.delete_all_channel_id)
    group_posts = await client(GetHistoryRequest(peer=channel, limit=30,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
    for message in group_posts.messages:
        if not message.raw_text:
            continue
        if message.raw_text.startswith('done'):
            break
        result = 0
        for tmp_client in clients[:1]: # only delete from main client since we disabled the auto forwarding
            result += await deleteTarget(tmp_client, message.raw_text)
        await client.edit_message(
            channel,
            message.id,
            text = 'done: %s deleted: %d' % (message.raw_text, result))

async def preProcess(clients, groups):
    for gid, setting in list(groups.items()):
        try:
            int(gid)
            continue
        except:
            ...
        _, client = getClient(clients, setting)
        group = await client.get_entity(gid)
        if group.username:
            setting['username'] = group.username
        if 'joinchat' in str(gid):
            setting['invitation_link'] = gid
        setting['title'] = group.title
        del groups[gid]
        groups[group.id] = setting
        with open('groups.yaml', 'w') as f:
            f.write(yaml.dump(groups, sort_keys=True, indent=2, allow_unicode=True))