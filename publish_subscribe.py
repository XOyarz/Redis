## chapter 3
import time
from redis import *

def publisher(n):
    time.sleep(1)
    for i in xrange(n):
        conn.publish('channel', i)
        time.sleep(1)


def run_pubsub():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0
    for item in pubsub.listen():
        print item
        count += 1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break

def notrans():
    print conn.incr('notrans:')
    time.sleep(.1)
    conn.incr('notrans:', -1)

if 1:
    for i in xrange(3):
        threadng.Thread(target=notrans).start()
    time.sleep(.5)