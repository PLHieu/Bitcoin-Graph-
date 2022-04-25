import aiohttp
# import asyncio
import time
import requests
# from asyncio import ClientSession
import threading


async def get_pokemon(session, url):
    async with session.get(url) as resp:
        pokemon = await resp.json()
        return pokemon['name']


def make_pokemon_requests(number):
    url = f'https://pokeapi.co/api/v2/pokemon/{number}'
    resp = requests.get(url)
    pokemon = resp.json()
    print(pokemon['name'])


# async def main():

#     async with aiohttp.ClientSession() as session:

#         tasks = []
#         for number in range(1, 151):
#             url = f'https://pokeapi.co/api/v2/pokemon/{number}'
#             tasks.append(asyncio.ensure_future(get_pokemon(session, url)))

#         original_pokemon = await asyncio.gather(*tasks)
#         for pokemon in original_pokemon:
#             print(pokemon)


start_time = time.time()
threads = []
for number in range(1, 151):
    t = threading.Thread(target=make_pokemon_requests, args=[number])
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print("--- %s seconds ---" % (time.time() - start_time))
