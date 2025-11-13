# README for mdoffload-python

Development test gRPC client and server for RGW metadata offload v1.

# Setup

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

# Server

```sh
# Run the server with defaults, plus auto-create buckets and
# objects (you normally want that).
$ ./mdoffload_server.py -v -O -B
```

# Client

```sh
# Set s3://test bucket attribute 'rhubarb' to value 'custard'. Use '-I' to
# generate a stable bucket ID.
$ ./mdoffload_client.py set-bucket-attributes -b test -I -A 'rhubarb=custard'

# Query that bucket attribute.
$ ./mdoffload_client.py get-bucket-attributes -b test -I
Attributes:
  rhubarb = custard
2025-11-13 15:21:23 ludwig-ub01.home.ae-35.com root[787941] INFO Success

# Delete that bucket attribute. '-A' and '-D' can be specified together.
# If they clash, the -A attributes are set first, then the -D attributes
# are deleted.
$ ./mdoffload_client.py set-bucket-attributes -b test -I -D 'rhubarb'

# Observe the deletion.
./mdoffload_client.py get-bucket-attributes -b test -I
Attributes:
2025-11-13 15:21:23 ludwig-ub01.home.ae-35.com root[787941] INFO Success

# Set s3://test/key1 object attribute 'foo' to value 'bar'. '-A' and '-D'
# work the same as for bucket attributes.
$ ./mdoffload_client.py set-object-attributes -b test -I -k key1 -A 'foo=bar'

# Query the object attribute.
$ ./mdoffload_client.py get-object-attributes -b test -I -k key1
{}
2025-11-11 13:06:43 ludwig-ub01.home.ae-35.com root[340567] INFO Success
{
  "attributes": {
    "foo": "bar"
  }
}
2025-11-11 13:06:43 ludwig-ub01.home.ae-35.com root[340590] INFO Success

# Set s3://test/key2 version ('instance') v1 object attribute 'baz' to value 'woof'.
$ ./mdoffload_client.py set-object-attributes -b test -I -k key2 -V v1 -A 'baz=woof'

# Query new attribute.
$ ./mdoffload_client.py get-object-attributes -b test -I -k key2 -V v1

Attributes:                                                                                                                                │```
  baz = woof

```

# Persistence

Data are persisted using SQLite3 databases, in `./data` by default. It's
achieved using sqlitedict with autocommit enabled, so it won't be fast.

To clear the data, delete `./data` yourself, or run

```sh
./mdoffload_server.py --reset
```

For obvious reasons, doing this while the server is running is a bad idea.