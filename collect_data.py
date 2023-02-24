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

def tweets_likes():
    tweets = pd.read_csv('~/data_science/analysis/twitter/data/company_tweets.csv')
    old_tweet_likes = pd.read_csv('~/data_science/analysis/twitter/data/tweet_likes.csv')
    mn_date = old_tweet_likes.merge(tweets[['id','timestamp']].rename(columns={'id':'tweet_id'})).timestamp.max()
    mn_date = datetime.strptime(mn_date, '%Y-%m-%d %H:%M:%S') - timedelta(days=3)
    mn_date = str(mn_date)
    tweets = tweets[(tweets.like_count > 2) & (tweets.timestamp >= mn_date) & (tweets.username != 'hellomoon_io')]
    tweets.groupby('like_count').count()
    tweets.groupby('username').count()

    it = 0
    tot = len(tweets)
    tweet_likes = pd.DataFrame()
    # for row in tweets.groupby('username').head(2).iterrows():
    for row in tweets.tail(tot - it + 1).iterrows():
        row = row[1]
        print('#{}/{}: {}'.format(it, tot, len(tweet_likes)))
        if row['username'] == 'hellomoon_io':
            it += 1
            continue
        url = 'https://api.twitter.com/2/tweets/{}/liking_users'.format(row['id'])
        params = {
            'max_results':100
        }
        has_more = True
        while has_more:
            sleep((15*60) / 75)
            try:
                json_response = connect_to_endpoint(url, params)
            except:
                sleep(10)
                json_response = connect_to_endpoint(url, params)
            if not 'data' in json_response.keys():
                print('Error {}'.format(row['username']))
                print(json_response)
                has_more = False
                continue
            cur = pd.DataFrame(json_response['data'])
            cur['account'] = row['username']
            cur['tweet_id'] = row['id']
            if len(cur) and 'next_token' in json_response['meta'].keys():
                params['pagination_token'] = json_response['meta']['next_token']
            else:
                has_more = False
            tweet_likes = pd.concat([tweet_likes, cur[['id','username','account','tweet_id']]])
            it += 1
    new_tweet_likes = pd.concat([old_tweet_likes, tweet_likes]).drop_duplicates()
    print('{} + {} -> {}'.format(len(old_tweet_likes), len(tweet_likes), len(new_tweet_likes)))
    new_tweet_likes.to_csv('~/data_science/analysis/twitter/data/tweet_likes.csv', index=False)

    tweet_likes = pd.read_csv('~/data_science/analysis/twitter/data/tweet_likes.csv')
    tweets = pd.read_csv('~/data_science/analysis/twitter/data/company_tweets.csv')
    tweets.sort_values('timestamp', ascending=0).head(20)
    tweets.timestamp.max()
    solana_core_audience = get_solana_core_audience(10)
    solana_core_audience = solana_core_audience.id.unique()
    df = tweet_likes[tweet_likes.id.isin(solana_core_audience)][['account','tweet_id']].merge(tweets[['id','timestamp']].rename(columns={'id':'tweet_id'}))
    df.groupby('account').tweet_id.count()
    def f(x):
        # days_since_monday = (x.weekday() + 1) % 7
        days_since_monday = (x.weekday()) % 7
        monday_of_week = x - timedelta(days=days_since_monday)
        return(monday_of_week)
    df['adj_date'] = df.timestamp.apply(lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + timedelta(hours=(15 - 5 + 48))).date() )
    df['adj_week'] = df.adj_date.apply(lambda x: f(x))
    # df = df.groupby(['account','adj_week']).timestamp.count().reset_index().rename(columns={'timestamp':'num_likes'})
    df = df.groupby(['account','adj_week']).timestamp.count().reset_index().rename(columns={'timestamp':'num_likes'})
    df = df[df.account != 'hellomoon_io']
    df.tail(20)
    df.to_csv('~/data_science/analysis/twitter/data/twitter_likes_data.csv', index=False)
