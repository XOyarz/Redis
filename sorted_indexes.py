## search and sort based on votes and updated times

def search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0,
                    start=0, num=20, desc=True):

    if id and not conn.expire(id, ttl):
        id = None

    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

        score_search = {
            id: 0,
            'sort:update': update,
            'sort:votes': vote
        }
        id = zintersect(conn, scored_search, ttl)

    pipeline = conn.pipeline(True)
    pipeline.zcard('idx:' + id)
    if desc:
        pipeline.zrevrange('idx:' + id, start, start + num - 1)
    else:
        pipeline.zrange('idx:' + id, start, start + num -1)
    results = pipeline.execute()

    return results[0], results[1], id

## helper functions for zset intersections and unions

def _zset_common(conn, method, scores, ttl=30, **kw):
    id = str(uuid.uuid4())
    execute = kw.pop('_execute', True)
    pipeline = conn.pipeline(True) if execute else conn
    for key in scores.keys():
        scores['idx:' + key] = scores.pop(key)

    getattr(pipeline, method) ('idx:' + id, scores, **kw)
    pipeline.expire('idx:' + id, ttl)
    if execute:
        pipeline.execute()
    return id

def zintersect(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)

def zunion(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)


## turn a string into a numeric score

def string_to_score(string, ignore_case=False):
    if ignore_case:
        string = string.lower()

    pieces = map(ord, string[:6])
    while len(pieces) < 6:
        pieces.append(-1)

    score = 0
    for piece in pieces:
        score = score * 257 + piece + 1

    return score * 2 + (len(string) > 6)