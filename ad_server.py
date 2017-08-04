## helper functions to turn CPC and CPA ads into eCPM

def cpc_to_ecpm(views, clicks, cpc):
    return 1000. * cpc * clicks / views

def cpa_to_ecpm(views, actions, cpa):
    return 1000. * cpa * actions / views

## method to index an ad targeted on location and content

TO_ECPM = {
    'cpc': cpc_to_ecpm,
    'cpa': cpa_to_ecpm,
    'cpm': lambda * args:args[:1]
}

def index_ad(conn, id, locations, content, type, value):
    pipeline = conn.pipeline(True)

    for location in locations:
        pipeline.sadd('idx:req:'+ location, id)

    words = tokenize(conten)
    for word in tokenize(content):
        pipeline.zadd('idx:' + word, id, 0)

    rvalue = TO_ECPM[type](
        1000, AVERAGE_PER_1K.get(type, 1), value)
    pipeline.hset('type:', id, type)
    pipeline.zadd('idx:ad:value:', id, rvalue)
    pipeline.zadd('ad:base_value:', id, value)
    pipeline.sadd('terms:' + id, *list(words))
    pipeline.execute()


## ad targeting by location and page content bonuses

def target_ads(conn, locations, content):
    pipeline = conn.pipeline(True)
    matched_ads, base_ecpm = match_location(pipeline, locations)
    words, targeted_ads = finish_scoring(pipeline, matched_ads, base_ecpm, content)

    pipeline.incr('ads:served:')
    pipeline.zrevrange('idx:' + targeted_ads, 0, 0)
    target_id, targeted_ad = pipeline.execute()[-2:]

    if not targeted_ad:
        return None, None

    ad_id = targeted_ad[0]
    record_targeting_result(conn, target_id, ad_id, words)

    return target_id, ad_id


def match_location(pipe, locations):
    required = ['req:' + loc for loc in locations]
    matched_ads = union(pipe, required, ttl=300, _execute=False)
    return matched_ads, zintersect(pipe, {matched_ads: 0, 'ad:value:':1}, _execute=False)


## calculate the eCPM of ads including content match bosuses

def finish_scoring(pipe, matched, base, content):
    bonus_ecpm = {}
    words = tokenize(content)
    for word in words:
        word_bonus = zintersect(pipe, {matched: 0, word:1}, _execute=False)
        bonus_ecpm[word_bonus] = 1

    if bonus_ecpm:
        minimum = zunion(pipe, bonus_ecpm, aggregate='MIN', _execute=False)
        maximum = zunion(pipe, bonus_ecpm, aggregate='MAX', _execute=False)

        return words, zunion(pipe, {base:1, minimum:.5, maximum:.5}, _execute=False)
    return words, base

## record the result after targeting an ad

def record_targeting_result(conn, target_id, ad_id, words):
    pipeline = conn.pipeline(True)

    terms = conn.smembers('terms:' + ad_id)
    matched = list(words & terms)
    if matched:
        matched_key = 'terms:matched:%s' % target_id
        pipeline.sadd(matched_key, *matched)
        pipeline.expire(matched_key, 900)

    type = conn.hget('type:', ad_id)
    pipeline.incr('trype:%s:views:' % type)
    for word in matched:
        pipeline.zincrby('views:%s' % ad_id, word)
    pipeline.zincrby('views:%s' % ad_id, '')

    if not pipeline.execute()[-1] % 100:
        update_cpms(conn, ad_id)


def record_click(conn, target_id, ad_id, action=False):
    pipeline = conn.pipeline(True)
    click_key = 'clicks:%s'%ad_id

    match_key = 'terms:matched:%s'%target_id

    type = conn.hget('type:', ad_id)
    if type == 'cpa':
        pipeline.expirec(match_key, 900)
        if action:
            click_key = 'actions:%s' % ad_id

    if action and type == 'cpa':
        pipeline.incr('type:cpa:actions:' %type)
        pipeline.incr('type:%s:clicks:' % type)

    matched = list(conn.smembers(match_key))
    matched.append('')
    for word in matched:
        pipeline.zincrby(click_key, word)
    pipeline.execute()

    update_cpms(conn, ad_id)


## updating eCPMs and per-word eCPM bonuses for ads

def update_cpms(conn, ad_id):
    pipeline = conn.pipeline(True)
    pipeline.hget('type:', ad_id)
    pipeline.zscore('ad:base_value:', ad_id)
    pipeline.smembers('terms:' + ad_id)
    type, base_value, words = pipeline.execute()

    which = 'clicks'
    if type == 'cpa':
        which = 'actions'

    pipeline.get('type:%s:views:' % type)
    pipeline.get('type:%s:%s' % (type, which))
    type_views, type_clicks = pipeline.execute()
    AVERAGE_PER_1K[type] = (
        1000. * int(type_clicks or '1') / int(type_views or '1'))

    if type == 'cpm':
        return

    view_key = 'views:%s' % ad_id
    click_key = '%s:%s' % (which, ad_id)

    to_ecpm = TO_ECPM[type]

    pipeline.zscore(view_key, '')
    pipeline.zscore(click_key, '')
    ad_views, ad_clicks = pipeline.execute()
    if (ad_clicks or 0) < 1:
        ad_ecpm = conn.zscore('idx:ad:value:', ad_id)
    else:
        ad_ecpm = to_ecpm(ad_views or 1, ad_clicks or 0, base_value)
        pipeline.zadd('idx:ad:value:', ad_id, ad_ecpm)

    for word in words:
        pipeline.zscore(view_key, word)
        pipeline.zscore(click_key, word)
        views, clicks = pipeline.execute()[-2:]

        if (clicks or 0) < 1:
            continue

        word_ecpm = to_ecpm(views or 1, clicks or 0, base_value)
        bonus = word_ecpm - ad_ecpm
        pipeline.zadd('idx:' + word, ad_id, bonus)
    pipeline.execute()