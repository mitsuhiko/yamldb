import os
import uuid
import weakref
import hashlib
import datetime
from itertools import chain
from contextlib import contextmanager
from collections import OrderedDict

import sqlite3

import yaml
from yaml.constructor import MappingNode, ConstructorError


def stringify(value):
    if value is None:
        return None
    if isinstance(value, (int, long)):
        return u'%016d' % value
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%dT%H:%M:%SZ')
    return unicode(value)


def get_file_hash(filename):
    with open(filename, 'rb') as f:
        d = hashlib.sha1()
        while 1:
            chunk = f.read(4096)
            if not chunk:
                break
            d.update(chunk)
        return d.hexdigest()


class OrderedLoader(yaml.SafeLoader):
    dict_class = OrderedDict

    def construct_mapping(self, node, deep=False):
        if not isinstance(node, MappingNode):
            raise ConstructorError(None, None,
                    "expected a mapping node, but found %s" % node.id,
                    node.start_mark)
        mapping = self.dict_class()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as exc:
                raise ConstructorError("while constructing a mapping", node.start_mark,
                        "found unacceptable key (%s)" % exc, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

    def construct_yaml_map(self, node):
        data = self.dict_class()
        yield data
        value = self.construct_mapping(node)
        data.update(value)


class OrderedDumper(yaml.SafeDumper):
    pass


OrderedLoader.add_constructor('tag:yaml.org,2002:map',
                              OrderedLoader.construct_yaml_map)
OrderedDumper.add_representer(OrderedDict, OrderedDumper.represent_dict)


class Query(object):

    def __init__(self, collection):
        self.collection = collection
        self._where_sql = []
        self._where_vars = []
        self._order_sql = []
        self._order_vars = []
        self._limit = None
        self._offset = None

    def filter(self, expr):
        sql, vars = expr.to_sql()
        self._where_sql.append(sql)
        self._where_vars.extend(vars)
        return self

    def order_by(self, expr):
        sql, vars = expr.to_sql()
        self._order_sql.append(sql)
        self._order_vars.extend(vars)
        return self

    def limit(self, value):
        if not hasattr(value, 'to_sql'):
            value = _Literal(value, stringify=False)
        self._limit = value
        return self

    def offset(self, value):
        if not hasattr(value, 'to_sql'):
            value = _Literal(value, stringify=False)
        self._offset = value
        return self

    def _make_select(self, columns=None):
        sql = 'select %s from "%s" where %s order by %s' % (
            ', '.join(columns or ['*']),
            self.collection.name,
            ' and '.join(self._where_sql),
            ', '.join(self._order_sql or ['_id'])
        )
        vars = list(chain(self._where_vars, self._order_vars))
        if self._limit is not None:
            limit_sql, limit_vars = self._limit.to_sql()
            sql += ' limit ' + limit_sql
            vars.extend(limit_vars)
            if self._offset is not None:
                offset_sql, offset_vars = self._offset.to_sql()
                sql += ' offset ' + offset_sql
                vars.extend(offset_vars)
        return sql, vars

    @contextmanager
    def _find_as_cursor(self, columns=None):
        sql, vars = self._make_select(columns)
        print sql, vars
        con = self.collection.database.get_index_db()
        cur = con.cursor()
        try:
            cur.execute(sql, vars)
            yield cur
        finally:
            con.close()

    def first(self):
        with self._find_as_cursor(['_id']) as cur:
            row = cur.fetchone()
            if row is not None:
                return self.collection.get(row[0])

    def all(self):
        result = []
        with self._find_as_cursor(['_id']) as cur:
            for row in cur.fetchall():
                rv = self.collection.get(row[0])
                if rv is not None:
                    result.append(rv)
        return result


class _QueryExpression(object):
    stringify_literal = False

    def to_sql(self):
        raise NotImplementedError()

    def __neg__(self):
        return _Neg(self)

    def __pos__(self):
        return self

    def _make_op(self, other, op):
        if not hasattr(other, 'to_sql'):
            other = _Literal(other, stringify=self.stringify_literal)
        return _Op(self, other, op)

    def __and__(self, other):
        return self._make_op(other, 'and')

    def __or__(self, other):
        return self._make_op(other, 'or')

    def __eq__(self, other):
        if other is None:
            return _IsNull(self, True)
        return self._make_op(other, '=')

    def __ne__(self, other):
        if other is None:
            return _IsNull(self, False)
        return self._make_op(other, '<>')

    def __gt__(self, other):
        return self._make_op(other, '>')

    def __ge__(self, other):
        return self._make_op(other, '>=')

    def __lt__(self, other):
        return self._make_op(other, '<')

    def __le__(self, other):
        return self._make_op(other, '<=')

    @property
    def year(self):
        return _Extract(self, '%Y')

    @property
    def month(self):
        return _Extract(self, '%m')

    @property
    def day(self):
        return _Extract(self, '%d')

    @property
    def date(self):
        return _Extract(self, '%Y-%m-%d')

    @property
    def hour(self):
        return _Extract(self, '%H')

    @property
    def minute(self):
        return _Extract(self, '%M')

    @property
    def second(self):
        return _Extract(self, '%S')


class _Extract(_QueryExpression):

    def __init__(self, expr, format):
        self.expr = expr
        self.format = format

    def to_sql(self):
        sql, vars = self.expr.to_sql()
        return 'cast(strftime(\'%s\', %s) as integer)' % (
            self.format,
            sql
        ), vars


class _Literal(_QueryExpression):

    def __init__(self, value, stringify=True):
        self.value = value
        self.stringify = stringify

    def to_sql(self):
        value = self.value
        if self.stringify:
            value = stringify(value)
        return '?', [value]


class _IsNull(_QueryExpression):

    def __init__(self, expr, is_null):
        self.expr = expr
        self.is_null = is_null

    def to_sql(self):
        sql, vars = self.expr.to_sql()
        return '%s %s' % (
            sql,
            self.is_null and 'is null' or 'is not null'
        )


class _Neg(_QueryExpression):

    def __init__(self, expr):
        self.expr = expr

    def to_sql(self):
        sql, vars = self.expr.to_sql()
        return sql + ' desc', vars


class _Op(_QueryExpression):

    def __init__(self, left, right, op):
        self.left = left
        self.right = right
        self.op = op

    def to_sql(self):
        sql_a, vars_a = self.left.to_sql()
        sql_b, vars_b = self.right.to_sql()
        return '(%s %s %s)' % (
            sql_a,
            self.op,
            sql_b
        ), chain(vars_a, vars_b)


class _Name(_QueryExpression):
    stringify_literal = True

    def __init__(self, name):
        self.name = name

    def to_sql(self):
        return '"%s"' % self.name, []


class _C(object):
    def __getattr__(self, name):
        return _Name(name)


C = _C()


class Database(object):
    loader_class = OrderedLoader
    dumper_class = OrderedDumper

    def __init__(self, folder):
        self.folder = folder
        self.collections = {}

    def get_index_db(self):
        try:
            os.makedirs(self.folder)
        except OSError:
            pass
        return sqlite3.connect(os.path.join(self.folder, '.indexes'))

    def reindex(self):
        con = self.get_index_db()
        cur = con.cursor()
        try:
            for collection in self.collections.itervalues():
                collection.reindex(cur)
                con.commit()
        finally:
            con.close()

    def get_collection(self, name):
        return self.collections[name]

    def declare_collection(self, name, indexes=None):
        if name in self.collections:
            raise ValueError('Collection defined more than once')
        rv = Collection(self, name, indexes)
        self.collections[name] = rv
        return rv


class Collection(object):
    extension = '.yml'
    query_class = Query

    def __init__(self, database, name, indexes=None):
        self._database = weakref.ref(database)
        self.name = name
        self.indexes = list(set(['_id']) | set(indexes or ()))
        self._ensure_indexes()

    def _ensure_indexes(self):
        con = self.database.get_index_db()
        try:
            cur = con.cursor()
            con.execute('create table if not exists "%s" (%s) ' % (
                self.name,
                ', '.join('%s text' % index for index in self.indexes + ['_hash'])
            ))
            for index in self.indexes:
                con.execute('create index if not exists index_%s_%s '
                            'on "%s" ("%s")' % (self.name, index,
                                                self.name, index))
            con.commit()
        finally:
            con.close()

    def reindex(self, cur):
        if not os.path.exists(self.path):
            return
        for filename in os.listdir(self.path):
            if not filename.endswith(self.extension):
                continue
            full_filename = os.path.join(self.path, filename)
            self._try_reindex_file(full_filename, filename.rsplit('.', 1)[0], cur)

    @property
    def database(self):
        return self._database()

    @property
    def path(self):
        return os.path.join(self.database.folder, self.name)

    def _force_folder(self):
        try:
            os.makedirs(self.path)
        except OSError:
            pass

    def _try_reindex_file(self, full_filename, id, cur):
        hash = get_file_hash(full_filename)
        cur.execute('select _hash from "%s" where _id = ?' % self.name, [id])
        row = cur.fetchone()
        if row is not None and row[0] == hash:
            return
        document = self.get(id)
        if document is None:
            return
        self._update_index_for(document, hash)

    def _update_index_for(self, document, hash, cur=None):
        id = document['_id']
        if cur is None:
            con = self.database.get_index_db()
            cur = con.cursor()
        else:
            con = None
        try:
            cur.execute('delete from "%s" where _id = ?' % self.name, (id,))
            values = []
            for index in self.indexes:
                values.append(stringify(document.get(index)))
            values.append(hash)
            cur.execute('insert into "%s" (%s, _hash) values (%s)' % (
                self.name,
                ', '.join('"%s"' % x for x in self.indexes),
                ', '.join(['?'] * (len(self.indexes) + 1))
            ), values)
            if con is not None:
                con.commit()
        finally:
            if con is not None:
                con.close()

    def get(self, id):
        """Return a document by primary key."""
        fn = os.path.join(self.path, id) + self.extension
        if os.path.isfile(fn):
            with open(fn, 'r') as f:
                rv = yaml.load(f, Loader=self.database.loader_class)
            rv['_id'] = id
            return rv

    def save(self, document):
        """Saves a document back."""
        if type(document) is dict:
            document = OrderedDict(sorted(document.items()))
        id = document.get('_id')
        if id is None:
            document['_id'] = id = str(uuid.uuid4())
        fn = os.path.join(self.path, id) + self.extension
        self._force_folder()
        with open(fn, 'w') as f:
            d = yaml.dump(document, Dumper=self.database.dumper_class,
                          allow_unicode=True, encoding='utf-8',
                          default_flow_style=False, width=74)
            f.write(d)
        self._update_index_for(document, hashlib.sha1(d).hexdigest())
        return document

    def delete(self, document):
        """Deletes a document again."""
        fn = os.path.join(self.path, id) + self.extension
        try:
            os.remove(fn)
        except OSError:
            return False
        con = self.database.get_index_db()
        cur = con.cursor()
        try:
            cur.execute('delete from %s where id = ?;' % self.name, (id,))
            con.commit()
        finally:
            con.close()
        return True

    @property
    def query(self):
        return self.query_class(self)
