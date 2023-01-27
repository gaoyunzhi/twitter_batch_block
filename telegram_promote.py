#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest, SearchRequest, SearchGlobalRequest
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import ChannelParticipantsKicked
from telethon.tl.types import InputMessagesFilterEmpty, ChannelParticipantsSearch, InputPeerEmpty
import asyncio
from datetime import datetime
import time
import sys
import random
from telegram_util import matchKey
from settings import Settings
from cache import Cache
from helper import getClient, preProcess, getPostIds, getPeerId, getDisplayLink, getLink, deleteAll, unpinTranslated, addChannel, deleteOld, checkUserID, kickAllInculdingChannels, checkUserChannel, checkMemberHistory, unkickAllInculdingChannels, replyTranslated, twitterBlock, twitterHideReply, logUserChannel, setStatsAdmin, addToOthersIndex, translateReview, recordHistory, markAsRead, setModerateAdmin, logChannelJoin, kickPersonalChannels
import hashlib
import time
import random

S = Settings()
C = Cache()

def shouldSend(messages, setting):
    for message in messages:
        if message.action:
            continue
        if time.time() - datetime.timestamp(message.date) < setting.get('wait_minute', 30) * 60:
            # print('need wait due to message', setting['title'], message.raw_text[:20])
            return False # 不打断现有对话
    if time.time() - datetime.timestamp(messages[0].date) > 24 * 60 * 60:
        return True
    for message in messages[:5]:
        if message.from_id and getPeerId(message.from_id) in S.promote_user_ids:
            return False
    return True

def getPromoteMessageHash(message):
    return '%s=%d=%d' % (message.split()[-1].split('/')[-1], datetime.now().month, int(datetime.now().day / 3))

def getMessageHash(post):
    message_id = post.grouped_id
    if post.fwd_from:
        message_id = message_id or post.fwd_from.channel_post
        return '%s=%s' % (str(getPeerId(post.fwd_from.from_id)), str(message_id))
    message_id = message_id or post.id
    return '%d=%d' % (getPeerId(post.peer_id), message_id)

def getHash(target, post):
    return '%s=%s' % (str(target), getMessageHash(post))

async def log(client, group, posts):
    debug_group = await C.get_entity(client, S.credential['debug_group'])
    await client.send_message(debug_group, getLink(group, posts[0]))

def getLogMessage(group, message, client_name):
    id_info, fwd_info, client_info, additional_info = '', '', '', ''
    additional_info = S.getAdditionalInfo(message)
    try:
        msg_id = message.peer_id.user_id
    except:
        msg_id = getPeerId(message.from_id)
    if msg_id:
        id_info = '[id](tg://user?id=%d): %d ' % (msg_id, msg_id)
    fwd_from = message.fwd_from and getPeerId(message.fwd_from.from_id)
    if fwd_from:
        fwd_info = 'fwd_id: %d ' % fwd_from
    if client_name != S.default_client_name:
        client_info = '%s ' % client_name
    return '%s%s%s%schat: %s' % (
        id_info,
        fwd_info,
        client_info,
        additional_info,
        getDisplayLink(group, message, S.groups))

def getShaHash(message):
    hash_content = [message.text, message.raw_text]
    if message.file:
        hash_content += [message.file.size, message.file.width, message.file.height]
    return hashlib.sha224(str(hash_content).encode('utf-8')).hexdigest()[:15]

def getItemHashs(message):
    yield 'forward=' + getShaHash(message)
    if message.raw_text:
        core = message.raw_text.split('user:')[0]
        if len(core) > 20:
            yield 'core='+ hashlib.sha224(str(core).encode('utf-8')).hexdigest()[:15]

def hashExistings(item_hashs):
    for item_hash in item_hashs:
        if item_hash and S.existing.get(item_hash):
            return True
    return False

async def getForwardGroup(client, message):
    forward_to_id =  S.groups[getPeerId(message.peer_id)].get('forward_to')
    if forward_to_id:
        return await C.get_entity(client, forward_to_id)
    tier = await S.getTier(message, client)
    if tier == 3: # save time
        return
    return await C.get_entity(client, S.credential['tiers'][tier])

async def logGroupPosts(client, group, group_posts, client_name):
    for message in group_posts.messages[::-1]:
        if S.isNoForwardMessage(message):
            continue
        if not message.raw_text and message.grouped_id:
            continue
        item_hashs = list(getItemHashs(message))
        if hashExistings(item_hashs):
            continue
        forward_group = await getForwardGroup(client, message)
        if not forward_group:
            continue
        tier = await S.getTier(message, client)
        post_ids = list(getPostIds(message, group_posts.messages))
        try:
            await client.forward_messages(forward_group, post_ids, group)
        except:
            ...
        log_message = getLogMessage(group, message, client_name)
        try:
            await client.send_message(forward_group, log_message, link_preview=False)
        except Exception as e:
            print('forward fail', str(e), tier, client_name, log_message)
            continue
        for item_hash in item_hashs:
            S.existing.update(item_hash, 1)

async def trySend(client, group, subscription, post):
    if time.time() - datetime.timestamp(post.date) < 5 * 60 * 60:
        return
    item_hash = getHash(group.id, post)
    if time.time() - S.message_log.get(getMessageHash(post), 0) < 12 * 60 * 60:
        return
    if S.existing.get(item_hash):
        return
    if S.shouldExclude(post):
        return
    post_ids = list(getPostIds(post, C.getPostsCached(subscription)))
    channel = await C.getChannel(client, subscription, S)
    S.existing.update(item_hash, -1)
    try:
        results = await client.forward_messages(group, post_ids, channel)
    except Exception as e:
        print('telegram_promote forward fail', group.title, subscription, post_ids, str(e))
        return
    print('promoted!', group.title)
    S.my_messages.add('%d_%d' % (group.id, results[0].id))
    await log(client, group, results)
    S.existing.update(item_hash, int(time.time()))
    return True

async def promoteSingle(client, group, setting):
    for subscription in setting.get('subscriptions', []):
        posts = await C.getPosts(client, subscription, S)
        for post in posts[:-9]:
            if not post.raw_text and not post.text:
                continue
            if setting.get('only_long'):
                if not post.raw_text:
                    continue
                text = post.raw_text.strip()
                text_byte_len = sum([1 if ord(c) <= 256 else 2 for c in text])
                if 'source' in text: 
                    text_byte_len += 19
                if text_byte_len <= 280:
                    continue
            result = await trySend(client, group, subscription, post)
            if result:
                return result

    if setting.get('subscriptions') and random.random() < 0.01:
        print('nothing to promote: %d %s' % (group.id, group.title))

    if not setting.get('message_loop_hard_promote'):
        return
    message = S.getPromoteMessage()
    item_hash = '%s=%s' % (str(group.id), getPromoteMessageHash(message))
    if S.existing.get(item_hash):
        return
    result = await client.send_message(group, message)
    print('promoted!', group.title)
    await log(client, group, [result])
    S.message_loop.inc('message_loop_hard_promote', 1)
    S.existing.update(item_hash, int(time.time()))
    return result

def getPurpose(promoted, setting, gid):
    if setting.get('always_log') or setting.get('forward_to'):
        return ['log'] # if later on we have two purpose group, we need to change here
    if promoted or not setting.get('promoting') or not S.shouldSendToGroup(gid, setting):
        return []
    return ['promote', 'log']

async def process(clients):
    targets = list(S.groups.items())
    random.shuffle(targets)
    promoted = False
    for gid, setting in targets:
        if setting.get('kicked'):
            continue
        purpose = getPurpose(promoted, setting, gid)
        if not purpose:
            continue
        client_name, client = getClient(clients, setting)
        try:
            group = await client.get_entity(gid)
        except Exception as e:
            print('telegram_promote Error group fetching fail', gid, setting, str(e))
            continue
        
        group_posts = await client(GetHistoryRequest(peer=group, limit=100,
            offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))
        if 'promote' in purpose and shouldSend(group_posts.messages, setting):
            result = await promoteSingle(client, group, setting)
            if result:
                promoted = True
        if 'log' in purpose:
            await logGroupPosts(client, group, group_posts, client_name)

async def forwardPrivateDialog(clients):
    for client_name, client in clients.items():
        try:
            dialogs = await client.get_dialogs()
        except:
            print('get_dialogs fail', client_name)
            continue
        forward_group = await C.get_entity(client, 1309758545)
        for dialog in dialogs:
            if not dialog.is_user:
                continue
            message = dialog.message
            if message.date and time.time() - datetime.timestamp(message.date) > 48 * 60 * 60:
                continue
            if S.existing_private_chat_user.contain(dialog.id):
                continue
            try:
                results = await client.forward_messages(forward_group, message.id, dialog.entity)
            except Exception as e:
                print('forwardPrivateDialog forward fail', str(e), client_name)
            log_message = getLogMessage(dialog.entity, message, client_name)
            try:
                await client.send_message(forward_group, log_message, link_preview=False)
            except Exception as e:
                print('forwardPrivateDialog log fail', str(e), client_name, log_message)
            S.existing_private_chat_user.add(dialog.id)

last_log = {'time': time.time()}
def logTimeElaspe(key):
    print(key, int(time.time() - last_log['time']))
    last_log['time'] = time.time()

async def run():
    clients = {}
    for user, setting in S.credential['users'].items():
        client = TelegramClient('session_file_' + user, S.credential['api_id'], S.credential['api_hash'])
        try:
            await client.start(password=setting.get('password'))
        except:
            print(user, setting)
        clients[user] = client 
    await checkUserID(clients, S, C)
    await checkUserChannel(clients['yun'])
    await logChannelJoin(clients['yun'])
    await kickPersonalChannels(clients, S)
    await kickAllInculdingChannels(clients, S)
    await unkickAllInculdingChannels(clients, S)
    if random.random() < 0.01:
        await addToOthersIndex(clients)
    if random.random() < 0.1:
        await preProcess(clients, S.groups)
        await addChannel(clients, S)
        await process(clients) # 67 seconds
    await unpinTranslated(clients['yun'])
    await translateReview(clients, 'yun', S)
    await replyTranslated(clients['yun']) # 14 seconds
    await deleteAll(clients, S)
    # await deleteOld(clients, S) # on demand
    await twitterBlock(clients)
    await twitterHideReply(clients['yun'])
    await checkMemberHistory(clients['yun'])
    # await recordHistory(clients['zhou'], S)
    await setStatsAdmin(clients)
    await setModerateAdmin(clients)
    await logUserChannel(clients, S)
    await markAsRead(clients, S)
    for _, client in clients.items():
        await client.disconnect()
    
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    r = loop.run_until_complete(run())
    loop.close()