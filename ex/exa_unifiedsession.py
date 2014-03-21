import time
import thredis


redis = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
redis.info()

redis.get('key1')
redis.set('key1', 'foooooo!!!!!')
redis.set('key2', 'value!!!!')
redis.get('key2')

redis.execute()
redis.get('key2')