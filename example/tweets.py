from datetime import datetime

from moesha.connection import Connection
from moesha.entity import Node, Relationship
from moesha.mapper import Mapper, EntityNodeMapper, EntityRelationshipMapper
from moesha.property import String, DateTime, RelatedEntity

from pypher.builder import __

# Create our connection and main mapper
connection = Connection(host='127.0.0.1', port='7687', username='neo4j',
    password='test')
mapper = Mapper(connection)


# Create the entities and their mappers
# Each entity must have a Mapper. The Mapper defines its properies 
# and relatiosnhips


class Tweeted(Relationship):
    pass


class TweetedMapper(EntityRelationshipMapper):
    entity = Tweeted


class Follows(Relationship):
    pass


class FollowsMapper(EntityRelationshipMapper):
    entity = Follows
    __PROPERTIES__ = {
        'since': DateTime(default=datetime.now),
    }


class User(Node):
    pass


class UserMapper(EntityNodeMapper):
    entity = User
    __PROPERTIES__ = {
        'username': String(),
    }
    __RELATIONSHIPS__ = {
        'Follows': RelatedEntity(relationship_entity=Follows,
            ensure_unique=True),
        'Followers': RelatedEntity(relationship_entity=Follows,
            ensure_unique=True, direction='in'),
        'Tweets': RelatedEntity(relationship_entity=Tweeted,
            ensure_unique=True),
    }


class Tweet(Node):
    pass


class TweetMapper(EntityNodeMapper):
    entity = Tweet
    __PROPERTIES__ = {
        'created': DateTime(default=datetime.now),
        'text': String(),
    }


# clear out all of the existing nodes
mapper.query(query='MATCH (n) detach delete n')

# time to create some entities and save them to the graph
mark = mapper.create(entity=User, properties={'username': 'mark'})
someone = mapper.create(entity=User, properties={'username': 'someone else'})

# lets save our users
work = mapper.save(mark, someone)
work.send()

# Mark should follow someone
user_mapper = mapper.get_mapper(User)
follow_relationship_entity, work = user_mapper(mark)['Follows'].add(someone)
work.send()

# let get a list of mark's followers and who he is following
followers = user_mapper(mark)['Followers']()
print('Mark\'s followers', followers.data)

follows = user_mapper(mark)['Follows']()
print('Mark follows', follows.data)

# do the same for someone
followers = user_mapper(someone)['Followers']()
print('Someone\'s followers', followers.data)

follows = user_mapper(someone)['Follows']()
print('Someone follows', follows.data)

# add a couple of tweets for each user
work = mapper.get_work()

for i in range(12):
    if i % 3:
        tweet1 = mapper.create(entity=Tweet, properties={
                'text': 'Mark tweet {}'.format(i),
            })
        _, work = user_mapper(mark)['Tweets'].add(tweet1, work=work)

    if i > 6:
        tweet2 = mapper.create(entity=Tweet, properties={
                'text': 'Someone tweet {}'.format(i),
            })
        _, work = user_mapper(someone)['Tweets'].add(tweet2, work=work)

work.send()


marks_tweets = user_mapper(mark)['Tweets']()
someones_tweets = user_mapper(someone)['Tweets']()

print('Mark\'s tweets', marks_tweets.data)
print('Someone\'s tweets', someones_tweets.data)