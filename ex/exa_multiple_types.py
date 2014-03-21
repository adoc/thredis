# Hashes
import thredis

s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
h = thredis.Hash('name', 'space', session=s)

id = h.set({'foo': True, 'bar': 'baz', 'boop': 123})
h.session.execute()

h.get(id)


# Sets
import thredis

s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
s.flushall()
se = thredis.Set('set', 'space', session=s)

se.add('set1')
se.add('set2')
se.add('set3')

se.session.execute()

se.all()

se.insert('set1.5', 1)

se.session.execute()

se.all()


# Collections
import thredis

s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
s.flushall()
c = thredis.Collection('coll', 'space', session=s)

id = c.add({'foo': 'bars'})

c.session.execute()

c.get(id)
