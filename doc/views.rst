Writing document design functions in Python
===========================================

The couchdb-python package comes with a query server to allow you to write
views and design functions in Python instead of JavaScript. When couchdb-python
is installed, it will install a script called couchpy that runs the query server.
To enable this for your CouchDB server, add the following section to local.ini::

    [query_servers]
    python=/usr/bin/couchpy

After restarting CouchDB, the Futon view editor should show ``python`` in
the language pull-down menu.

The Python query server supports command line arguments which helps to customize
it behavior or extend available features:

    - ``--json-module=<name>``
      Set the JSON module to use ('simplejson', 'cjson', or 'json' are supported)
    - ``--log-file=<file>``
      Log file path to handle query server logging output.
    - ``--log-level=<level>``
      Specifies query server logging level (debug, info, warn, error).
      Uses info level by default.
    - ``--enable-eggs``
      Enables support of :ref:`modules_eggs` as modules.
    - ``--egg-cache=<path>``
      Specifies egg cache dir. If omitted, ``PYTHON_EGG_CACHE`` environment
      variable value would be used or system temporary directory if variable not
      setted.
    - ``--allow-get-update``
      This option allows to use ``GET`` requests to call :ref:`updates` functions
    - ``--couchdb-version=<ver>``
      Defines with which version of couchdb server query server works. This
      option controls supported features and their behavior. If omitted query
      server would try to use all implemented features for the latest supported
      version.

.. note:: All versions notes are for CouchDB, not couchdb-python.

Context
-----------------------

Each design function executes within special context of predefined objects,
modules and functions. This context helps to operate

    - :func:`~logging.log`: Message logger to output stream on info level.
      Operates on ``couchdb.server.design_function`` channel. Note, that this
      messages writes both into CouchDB server and couchdb-python logs.
    - :mod:`~couchdb.json`: Active couchdb-python json module.
    - :exc:`~couchdb.server.exceptions.FatalError`: Fatal exception which
      would terminate query server.
    - :exc:`~couchdb.server.exceptions.Error`: Non fatal exception which
      terminates current operation, but not query server.
    - :exc:`~couchdb.server.exceptions.Forbidden`: Non fatal exception which
      signs access violation and doesn't terminate query server. Generates
      warning log message instead of error.
    - :func:`~couchdb.server.mime.register_type`: Registers mimetypes by
      associated key.
    - :func:`~couchdb.server.mime.provides`: Registers handler for specified
      mime type key.
    - :func:`~couchdb.server.render.start`: Initiates chunked response.
    - :func:`~couchdb.server.render.send`: Sends response chunk.
    - :func:`~couchdb.server.render.get_row`: Extracts next row from view result.
    - :func:`~couchdb.server.compiler.require`: Provides access to
      :ref:`cjs_modules`.

.. versionchanged:: 0.9.0
    Added :func:`~couchdb.server.mime.provides`
    and :func:`~couchdb.server.mime.register_type` mime functions.
.. versionchanged:: 0.9.0
    Added :func:`~couchdb.server.render.response_with` function.
.. versionchanged:: 0.10.0
    Removed :func:`~couchdb.server.render.response_with` function.
.. versionchanged:: 0.10.0
    Added functions: :func:`~couchdb.server.render.start`,
    :func:`~couchdb.server.render.send`,
    :func:`~couchdb.server.render.get_row`
.. versionchanged:: 0.11.0
    Added :func:`~couchdb.server.compiler.require` function.

Views
-----------------------

.. _map:

Map
^^^^^^^^^^^^^^^^^^^^^^^

Map functions should take single argument as document dict object and emit
two value list or tuple of key-value result. Normally, you would like to
use yield statement for emitting result:

.. code-block:: python

    def mapfun(doc):
        doc_has_tags = isinstance(doc.get('tags'), list)
        if doc['type'] == 'post' and doc_has_tags:
            for tag in doc['tags']:
                yield tag.lower(), 1

Note that the ``map`` function uses the Python ``yield`` keyword to emit
values, where JavaScript views use an ``emit()`` function. However, you are free
to use ``return`` instead of ``yield``:

.. code-block:: python

    def mapfun(doc):
        doc_has_tags = isinstance(doc.get('tags'), list)
        if doc['type'] == 'post' and doc_has_tags:
            return [[tag.lower(), 1] for tag in doc['tags']]

But you should remember, that emitting huge result in one shot consumes much
more memory than yielding it step by step.

Each document object is `sealed` which means that it could changed without worry
that next ``map`` function receives it in modified state.

Reduce and rereduce
^^^^^^^^^^^^^^^^^^^^^^

Reduce functions takes two required arguments of keys and values lists - the
result of map function - and optional third which signs if rereduce mode is
active or not. There is third optional argument `rereduce` which signs is
rereduce mode active or not.

If ``reduce`` function result is twice longer than initial request than
:exc:`~couchdb.server.exceptions.Error` exception would be raised.
However, this behavior could be disabled by setting reduce_limit to False
in CouchDB sever config (see query_server_config options section).

Remember that since CouchDB 0.11.0 version there are several builtin
reduce functions that runs much faster than Python's one:

.. code-block:: python

    # could be replaced by _sum
    def reducefun(keys, values):
        return sum(values)

    # could be replaced by _count
    def reducefun(keys, values, rereduce):
        if rereduce:
            return sum(values)
        else:
            return len(values)

    # could be replaced by _stats
    def reducefun(keys, values):
        return {
            'sum': sum(values),
            'min': min(values),
            'max': max(values),
            'count': len(values),
            'sumsqr': sum(v*v for v in values)
        }


Common objects
-----------------------

Before you learn more design functions, there are some objects that are wide
used by them. Let's take a look on this objects.

.. _request_object:

Request object
^^^^^^^^^^^^^^^^^^^^^^^

`Request object` is dict which contains request information data. It forms from
the actual HTTP request to CouchDB and some internal data which helps in request
procession:

    - info (`dict`): :ref:`dbinfo`.
    - id (`unicode`): Requested document id if it was or None.
    - uuid (`unicode`): UUID string generated for this request.
    - method (`unicode` or `list`): Request method as unicode string for
      `HEAD`, `GET`, `POST`, `PUT`, `DELETE`, `OPTIONS` and `TRACE` values and
      as list of char codes for others.
    - requested_path: Actual requested path if it was rewritted.
    - path (`list`): List of path string chunks.
    - query (`dict`): URL query parameters. Note that multiple keys not
      supported and last key value suppress others.
    - headers (`dict`): Request headers.
    - body (`unicode`): Request body. For `GET` requests contains ``undefined``
      string value.
    - peer (`unicode`): Request source IP address.
    - form (`dict`): Decoded body to key-value pairs if `Content-Type` header
      is ``application/x-www-form-urlencoded``.
    - cookie (`dict`): Related cookies.
    - userCtx (`dict`): :ref:`userctx`.
    - secObj (`dict`): :ref:`secobj`.

.. versionadded:: 0.9.0
.. versionchanged:: 0.10.0 Add ``userCtx`` field.
.. versionchanged:: 0.11.0 Rename ``verb`` field to ``method``.
.. versionchanged:: 0.11.0 Add ``id`` and ``peer`` fields.
.. versionchanged:: 0.11.1 Add ``uuid`` field.
.. versionchanged:: 1.1.0 Add ``requested_path`` and ``secObj`` fields.

.. _response_object:

Response object
^^^^^^^^^^^^^^^^^^^^^^^

`Response object` as dict object that design functions (actually, :ref:`render`
ones) should return to CouchDB which transforms them into fulfill HTTP response:

    - code (`int`): Response HTTP status code.
    - json (`dict or list`): JSON encodable object. Automatically sets
      `Content-Type` header as ``application/json``.
    - body (`unicode`): Unicode response string. Automatically sets `Content-Type`
      header as ``text/html; charset=utf-8``.
    - base64 (`string`): Base64 encoded string. Automatically sets `Content-Type`
      header as ``application/binary``.
    - headers (`dict`): Response headers dict. `Content-Type` headers from this
      set overrides any automatically assigned one.
    - stop (`bool`): Signal for lists to stop iteration over view result rows.

Note, that ``body``, ``base64`` and ``json`` keys are overlaps each other and
the last wins. However, due to Python doesn't keep dict keys original order this
could create a confusing situation. Try to use only one of them.

Any other dict key would raise CouchDB internal exception.
Also `Response object` could be a simple unicode string value which would be
automatically wrapped into ``{'body': ...}`` dict.

.. _dbinfo:

Database information
^^^^^^^^^^^^^^^^^^^^^^^

This dictionary hold information about database:

  - db_name (`unicode`): Database name.
  - doc_count (`int`): Document count.
  - doc_del_count (`int`): Count of deleted documents.
  - update_seq (`int`): Count of updated sequences.
  - purge_seq (`int`):  Purged sequences count.
  - compact_running (`bool`): Compact running flag.
  - disk_size (`int`): Database size in bytes.
  - instance_start_time (`unicode`): When CouchDB server have been started.
  - disk_format_version (`int`): Database file format.
  - commited_update_seq (`int`): Committed sequences on disk.

Same information could be also retrieved by HTTP request::

    GET http://couchdbserver:5984/dbname

.. versionchanged:: 0.9.0 
    Added ``db_name``, ``purge_seq``, ``instance_start_time`` fields.
.. versionchanged:: 0.10.0
    Added ``disk_format_version`` field.
.. versionchanged:: 1.0.1
    Added ``commited_update_seq`` field.

.. _userctx:

User context
^^^^^^^^^^^^^^^^^^^^^^^

User context (``userCtx``) is a `dict` object contained information about
current CouchDB user, name and roles it has:

    - db (`unicode`): Current database name.
    - name (`unicode`): User name.
    - roles (`list`): List of user roles.

For example, if name is ``None`` and ``_admin`` in `roles` so there might be
admin party.

This information could be also retrieved by HTTP request::

    GET http://couchdbserver:5984/_session

.. _secobj:

Security object
^^^^^^^^^^^^^^^^^^^^^^^

Security object (``secobj``) is a `dict` holds database security information
about who is admins and who is just readers:

    - admins (`dict`): Information about database admins with keys:
        - names (`list`): List of user names.
        - roles (`list`): List of role names.
    - readers (`dict`): Information about database readers with keys:
        - names (`list`): List of user names.
        - roles (`list`): List of role names.

This information could be also retrieved by HTTP request::

    GET http://couchdbserver:5984/dbname/_security

.. _cjs_modules:

Modules
^^^^^^^^^^^^^^^^^^^^^^^

Modules are the major CouchDB feature since 0.11.0 version which allows to
create modular design functions without needs to duplicate a lot of same
functionality. This is implementation of CommonJS
`Modules <http://wiki.commonjs.org/wiki/Modules/1.1.1>`_ specification by
:func:`~couchdb.server.compiler.require` function which is available for all
:ref:`ddoc` functions.

Example of stored module:

.. code-block:: python

    class Validate(object):
        def __init__(self, newdoc, olddoc, userctx):
            self.newdoc = newdoc
            self.olddoc = olddoc
            self.userctx = userctx

        def is_author():
            return self.doc['author'] == self.userctx['name']

        def is_admin():
            return '_admin' in self.userctx['roles']

        def unchanged(field):
            assert (self.olddoc is not None
                    and self.olddoc[field] == self.newdoc[field])

    exports['init'] = Validate

Each stored modules have access to additional global variables:

    - module (`dict`): Contains information about stored module.
        - id (`unicode`): Module id by which it always could be "required".
        - current (`code`): Module compiled code object.
        - parent (`dict`): Parent frame.
        - exports (`dict`): Exported statements which would be accessible within
          design functions.
    - require (`function`): Require function with relative point started at
      current module.
    - exports (`dict`): Shortcut to ``module['exports']`` dictionary.

Lets place module above within design document under "lib/validate" path. This
path should be readed as "there is field `lib` in design document that is the
object and has field `validate`". Now this module could be used in next way:

.. code-block:: python

    def validate_doc_update(newdoc, olddoc, userctx):
        init_v = require('lib/validate')['init']
        v = init_v(newdoc, olddoc, userctx)

        if v.is_admin():
            return True

        v.unchanged('author')
        v.unchanged('created_at')
        return True

.. versionadded:: 0.11.0
.. versionchanged:: 1.1.0
    Avaiable for :ref:`map` functions if ``add_lib`` command proceeded.

.. _modules_eggs:

Eggs
^^^^^^^^^^^^^^^^^^^^^^^

As unique feature of Python query server there is support of
`eggs <http://peak.telecommunity.com/DevCenter/PythonEggs>`_ as modules. This
feature could be activated manualдy by query server ``--enable-eggs`` command
line argument due to compatibility and security reasons: eggs could contains
a very complex code that could be revised from the first sight.

Such egg-modules should be stored as base64 encoded strings, which could be
successful decoded by :func:`base64.b64decode` function.

For Python 2.4 version `setuptools <http://pypi.python.org/pypi/setuptools>`_
package is the additional requirement.

.. _shows:

Shows
-----------------------

Show functions are used to represent documents in various formats, commonly as
HTML page with nicer formatting.

Show function should return :ref:`response_object` and take two arguments:
    - doc (`dict`): Document object.
    - req (`dict`): :ref:`request_object`.

Basic example of show function could be:

.. code-block:: python

    def show(doc, req):
        return {
            'code': 200,
            'headers': {
                'X-CouchDB-Python': '0.9.0'
            },
            'body': 'Hello, World!'
        }

Also, there is more simple way to return json encoded data:

.. code-block:: python

    def show(doc, req):
        return {
            'json': {
                'id': doc['_id'],
                'rev': doc['_rev'],
                'type': doc['type']
            }
        }

and even files (this one is CouchDB logo):

.. code-block:: python

    def show(doc, req):
        return {
            'headers': {
                'Content-Type' : 'image/png',
            },
            'base64': ''.join([
                'iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAsV',
                'BMVEUAAAD////////////////////////5ur3rEBn////////////////wDBL/',
                'AADuBAe9EB3IEBz/7+//X1/qBQn2AgP/f3/ilpzsDxfpChDtDhXeCA76AQH/v7',
                '/84eLyWV/uc3bJPEf/Dw/uw8bRWmP1h4zxSlD6YGHuQ0f6g4XyQkXvCA36MDH6',
                'wMH/z8/yAwX64ODeh47BHiv/Ly/20dLQLTj98PDXWmP/Pz//39/wGyJ7Iy9JAA',
                'AADHRSTlMAbw8vf08/bz+Pv19jK/W3AAAAg0lEQVR4Xp3LRQ4DQRBD0QqTm4Y5',
                'zMxw/4OleiJlHeUtv2X6RbNO1Uqj9g0RMCuQO0vBIg4vMFeOpCWIWmDOw82fZx',
                'vaND1c8OG4vrdOqD8YwgpDYDxRgkSm5rwu0nQVBJuMg++pLXZyr5jnc1BaH4GT',
                'LvEliY253nA3pVhQqdPt0f/erJkMGMB8xucAAAAASUVORK5CYII='])
        }

.. versionadded:: 0.9.0

.. seealso::

    CouchDB Wiki:
        `Showing Documents <http://wiki.apache.org/couchdb/Formatting_with_Show_and_List#Showing_Documents>`_

    CouchDB Guide:
        `Show Functions <http://guide.couchdb.org/editions/1/en/show.html>`_

Lists
-----------------------

When ``show`` functions used to customize document presentation, ``list`` ones
are used for same task, but for :ref:``views`` result.

Lists protocol had been heavy changed between CouchDB 0.9.0 and 0.10.0 versions.

For CouchDB 0.9 ``list`` function takes four arguments:
    - head (`dict`): View result information.
    - row (`dict`): View result row.
    - req (`dict`): :ref:`request_object`.
    - row_info (`dict`): Object with information about the iteration state.

and always should return :ref:`response_object`:

.. code-block:: python

    def listfun(head, row, req, info):
        if head is not None:
            return {
                'headers': {
                    'Content-Type': 'text/html'
                }
            }
        elif row is not None:
            return row['value']
        else:
            return ''


Since CouchDB 0.10 ``list`` function takes only two arguments:
    - head (`dict`): View result information.
    - req (`dict`): :ref:`request_object`.

and example above would be next:

.. code-block:: python

    def listfun(head, req):
        start({
            'headers': {
                'Content-Type': 'text/html'
            }
        })
        for row in get_row():
            send(row['value'])

Note, that :func:`~couchdb.server.render.get_row` is a generator, which yields
views rows, not a function as for javascript.

.. versionadded:: 0.9.0
.. versionchanged:: 0.10.0
    Uses new API. See CouchDB documentation for more information.

.. seealso::

    CouchDB Wiki:
        `Listing Views with CouchDB 0.9 <http://wiki.apache.org/couchdb/Formatting_with_Show_and_List#Listing_Views_with_CouchDB_0.9>`_
        `Listing Views with CouchDB 0.10 and later <http://wiki.apache.org/couchdb/Formatting_with_Show_and_List#Listing_Views_with_CouchDB_0.10_and_later>`_
        
    CouchDB Guide:
        `Transforming Views with List Functions <http://guide.couchdb.org/draft/transforming.html>`_

.. _updates:

Updates
-----------------------

``Update`` functions allows to perform document creation or updation operations
with custom complex logic which runs on CouchDB server side. By default, ``GET``
method is not allowed to these functions, but you may remove this behavior by
passing``--allow-get-update`` argument to query server.

``update`` function should take two arguments:
    - doc (`dict`): Document object.
    - req (`dict`): :ref:`request_object`.

Return value should be a two element list of `document` and :ref:`response_object`.

| If the `document` is ``None`` than nothing will be committed to the database.
| If `document` exists, it should already have an `_id` and `_rev` fields setted.
| If `document` doesn't exists it will be created.

.. code-block:: python

    def update(doc, req):
        if not doc:
            if 'id' in req:
                # create new document
                return [{'_id': req['id']}, 'New World']
            # change nothing in database
            return [None, 'Empty World']
        doc['world'] = 'hello'
        doc['edited_by'] = req.get('userCtx')
        # update document in database
        return [doc, 'Hello, World!']

.. versionadded:: 0.10.0

.. seealso::

    CouchDB Wiki:
        `Document Update Handlers <http://wiki.apache.org/couchdb/Document_Update_Handlers>`_

Filters
-----------------------

``filter`` functions wide used with ``_changes`` feed and replications,
extracting only sequences that has matched by function.

``filter`` function takes 2 arguments:
    - doc (`dict`): Document which is proceed by filter.
    - req (`dict`): :ref:`request_object`.

And should return boolean value, where ``True`` means that document have passed
through filter and ``False`` if not.

.. code-block:: python

    def filterfun(doc, req):
        return doc.get('type', '') == 'post'

To make ``filter`` function compatible with old CouchDB servers, third argument
must be setted as optional:

.. code-block:: python

    def filterfun(doc, req, userctx=None):
        if userctx is None:
            userctx = req['userCtx']
        return doc.get('type', '') == 'post' and 'writer' in userctx['role']

.. versionadded:: 0.10.0
.. versionchanged:: 0.11.1 Argument userctx no longer have passed.
    Use ``req['userCtx']`` instead.

.. seealso::

    CouchDB Guide:
      `Guide to filter change notification <http://guide.couchdb.org/draft/notifications.html#filters>`_

    CouchDB Wiki:
      `Filtered replication <http://wiki.apache.org/couchdb/Replication#Filtered_Replication>`_

Validate
-----------------------

To perform validate operations on document saving there is special design
function type called ``validate_doc_update``.

This function should take next four arguments:
    - newdoc (`dict`): Changed document object.
    - olddoc (`dict`): Original document object or None if it is new.
    - userctx (`dict`): :ref:`userctx`
    - secobj (`dict`): :ref:`secobj`

However, since ``secobj`` argument doesn't mentioned in most part of
documentation nor examples it leaved as optional, but with cost of warning
message in logs.

``validate_doc_update`` functions should raise
:exc:`~couchdb.server.exceptions.Forbidden` exception to prevent document
storing within database. Builtin ``AssertionError`` exception works in same way.

Example (for 0.11.1+):

.. code-block:: python

    def validate_post_update(newdoc, olddoc, userctx, secobj):
        # of course you should also check roles
        if userctx['name'] not in secobj['admins']:
            assert newdoc['author'] == userctx['name']
        return True

Another example, more complex and portable:

.. code-block:: python

    def validate_post_update(newdoc, olddoc, userctx, secobj=None):
        if newdoc.get('type') != 'post' or not olddoc:
            return True
        username = userctx['name']
        skip_check_authorship = False
        if secobj is not None:
            skip_check_authorship |= userctx['name'] in secobj['admins']['names']
            skip_check_authorship |= userctx['role'] in secobj['admins']['roles']
        skip_check_authorship |= 'editor' in userctx['roles']
        skip_check_authorship |= '_admin' in userctx['roles']
        if not skip_check_authorship:
            assert newdoc['author'] == username
        return True

Note, that return statement used only for function exiting and it doesn't
controls validate state.

.. versionadded:: 0.9.0
.. versionchanged:: 0.11.1 Added argument ``secobj``.

.. seealso::

    CouchDB Guide:
      `Validation Functions <http://guide.couchdb.org/editions/1/en/validation.html>`_

    CouchDB Wiki:
      `Document Update Validation <http://wiki.apache.org/couchdb/Document_Update_Validation>`_
