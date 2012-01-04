Kademlia
==========

A Kademlia DHT implementation in node, ready to be used as
a distributed data store.

Install using `npm install kademlia`

Use:

    var dht = require('kademlia')
    var node = new dht.KNode({ address: 'IP address', port: portNumber });
    node.connect('existing peer ip', port);
    node.set('key', data);

    node.get('key', function(err, data) {
        console.log("Retrieved", data, "from DHT");
    });

API
---

### KNode

The KNode represents a Kademlia node and handles all communication and storage.
This should be the only thing you need to interact with the Kademlia overlay
network.

#### constructor(configuration)

A KNode is created by passing it an object having `address` and `port`
properties. The node will bind to `port` and start running.

    var node = new dht.KNode({ address: '10.100.98.60', port: '12345' });

#### connect(address, port)

Used to introduce this Kademlia node to the overlay network. If you know the
address and port of an existing Kademlia peer, you may `connect` to it so that
this node can become part of the network.

    node.connect('10.100.98.12', 42922);

#### get(key, callback)

Gets the value associated with `key` from the Kademlia network. `callback` is
a function with arguments `(err, value)`. If the value is found, `err` is
`null`, otherwise `err` will be an object containing information about what
went wrong and `value` will be `null`.

    node.get('foo', function(err, value) {
        if (err) {
            // something went wrong
            return;
        }

        // use value
    });

#### set(key, value[, callback])

Store the `key`, `value` pair in the Kademlia network. `set()` is not
guaranteed to succeed. `callback` can be used to check the result of the store.
It is `function (err)`. If the store succeeded, `err` is `null`, otherwise
`err` describes what went wrong.

    node.set('foo', 'bar', function(err) {
        if (err) {
            // might want to try again
        }
    });

#### self

An object describing this node.

    {
        nodeID: 'SHA1 hash, unique ID of the node',
        address: 'IP address the node is on',
        port: port number (integer) the node is bound to
    }

Contributors
------------

Maintainer: Nikhil Marathe <nsm.nikhil@gmail.com>
Contributors: https://github.com/nikhilm/kademlia/contributors

License
-------

Kademlia is distributed under the MIT License.
