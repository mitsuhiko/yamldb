from datetime import datetime
from yamldb import Database, C


db = Database('.')
posts = db.declare_collection('posts', ['pub_date', 'slug'])

def create_posts():
    posts.save({'slug': 'post1', 'title': "This is the first post",
                'pub_date': datetime(2011, 1, 1, 17, 23)})
    posts.save({'slug': 'post2', 'title': "This is the second post",
                'pub_date': datetime(2011, 2, 1, 12, 30)})

def test_queries():
    print posts.query.filter(C.pub_date.year == 2011).all()
