import requests
import pandas as pd
from time import sleep
from datetime import datetime

# TODO
bearer_token = '_____'

########################################
#     Twitter API Helper Functions     #
########################################
def bearer_oauth(r):
    '''
    Method required by bearer token authentication.
    '''

    r.headers['Authorization'] = f'Bearer {bearer_token}'
    r.headers['User-Agent'] = 'v2FollowersLookupPython'
    return(r)

def connect_to_endpoint(url, params):
    response = requests.request('GET', url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            'Request returned an error: {} {}'.format(
                response.status_code, response.text
            )
        )
    return(response.json())

############################################
#     Get Twitter Handles for NFT PFPs     #
############################################
def get_pfp_handles():
    collections = [
        'abc_abracadabra',
        'aurory',
        'blocksmith_labs',
        'boogle_gen_1',
        'boryoku_dragonz',
        'bubblegoose_ballers',
        'cets_on_creck',
        'claynosaurz',
        'communi3',
        'degenerate_ape_academy',
        'degenerate_trash_pandas',
        'degods',
        'famous_fox_federation',
        'galactic_geckos',
        'ghost_kid_dao',
        'justape',
        'lily',
        'ls_the_hallowed',
        'monkey_baby_business',
        'okay_bears',
        'primates',
        'rascals',
        'shadowy_super_coder_dao',
        'solana_monke_rejects',
        'solana_monkey_business',
        'solgods',
        'stoned_ape_crew',
        'taiyo_robotics',
        'the_catalina_whale_mixer',
        'thugbirdz',
        'trippin_ape_tribe',
        'y00ts'
    ]
    tot = len(collections)
    it = 1
    members = pd.DataFrame()
    for c in collections:
        print('#{} / {}: {} ({})'.format(it, tot, c, len(members)))
        sleep(0.25)
        url = 'https://www.nftinspect.xyz/api/collections/members/{}?limit=5000&onlyNewMembers=false&withNonPFP=false&sortMode=FOLLOWED'.format(c)
        r = requests.get(url)
        j = r.json()
        m = pd.DataFrame(j['members'])
        m['collection'] = c
        members = pd.concat([members, m])
        it += 1
    members = members[members.collection != 'ls_the_hallowed']
    members.to_csv('~/git/nft_social_score/members.csv', index=False)

############################################
#     Get all tweets sent out by users     #
############################################
def user_tweets():
    members = pd.read_csv('~/git/nft_social_score/members.csv')
    members = members[members.followedCoefficientWithVerified >= 0.01]
    members.groupby('collection').id.count().reset_index().sort_values('id')
    members = members[members.collection == 'hot_heads']

    prv = ''
    it = 0
    tot = len(members)
    tweets = pd.DataFrame()
    for row in members.iterrows():
        sleep(900.0 / (15*60))
        print('#{}/{}: {}'.format(it, tot, len(tweets)))
        row = row[1]
        url = 'https://api.twitter.com/2/users/{}/tweets'.format(row['id'])
        params = {
            'exclude': 'retweets'
            , 'max_results':100
            , 'tweet.fields': 'created_at,public_metrics'
        }
        has_more = True
        while has_more:
            try:
                json_response = connect_to_endpoint(url, params)
            except:
                sleep(10)
                json_response = connect_to_endpoint(url, params)
            if not 'data' in json_response.keys():
                print('Error {}'.format(row['id']))
                has_more = False
                continue
            cur = pd.DataFrame(json_response['data'])
            cur['timestamp'] = cur.created_at.apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))
            cur['days_ago'] = (datetime.today() - cur.timestamp).apply(lambda x: x.days )
            if len(cur) and cur.days_ago.values[-1] <= 7 and 'next_token' in json_response['meta'].keys():
                params['pagination_token'] = json_response['meta']['next_token']
            else:
                has_more = False
            cur['user_id'] = row['id']
            cur['username'] = row['username']
            cur['collection'] = row['collection']
            tweets = pd.concat([tweets, cur])
        if row['collection'] != prv:
            prv = row['collection']
            tweets['like_count'] = tweets.public_metrics.apply(lambda x: x['like_count'] )
            tweets['tweet_count'] = 1
            print(tweets[tweets.days_ago <= 7].groupby(['collection'])[['like_count','tweet_count']].sum())
            del tweets['like_count']
            del tweets['tweet_count']
            tweets.to_csv('~/git/nft_social_score/tweets.csv', index=False)
        it += 1

