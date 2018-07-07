/*
 * Copyright (C) 2011-2012 by Nikhil Marathe <nsm.nikhil@gmail.com>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
"use strict";

let assert		= require('assert');
let node_constants	= require('constants');
let async		= require('async');
let constants		= require('./constants');
let util		= require('./util');
let rpc			= require('./rpc');
let Bucket		= require('./bucket').Bucket;
let _			= require('underscore');
let _s			= require('string');


// abbreviate message utilities
let MC = util.message_contact;
let MID = util.message_rpcID;


/**
 *	class of KNode
 */
class KNode
{
	constructor( desc )
	{
		this.m_oThis	= this;

		//	TODO: probably want to persist nodeID
		this.self = _.defaults({ nodeID: util.nodeID(desc.address, desc.port) }, desc);
		Object.freeze( this.self );

		this._storage = {};
		// object treated as an array
		this._buckets = {};
		this._rpc = new rpc.RPC(this.self, _.bind(this._onMessage, this));
	}



	_MSG( type, params )
	{
		// NOTE: always keep this.self last. This way users of _MSG
		// don't have to worry about accidentally overriding self properties
		return _.extend( { type: type }, params, this.self );
	}

	_onMessage( message )
	{
		if ( ! message.type || typeof message.type !== 'string' )
		{
			return;
		}

		let sMethodName	= message.type.toLowerCase();
		sMethodName	= _s( '_' + sMethodName ).camelize();
		sMethodName	= '_on' + sMethodName;

		//	_onFindNode
		let pfnAction = this[ sMethodName ];
		if ( pfnAction )
		{
			this._updateContact( MC( message ) );

			//	...
			switch ( sMethodName )
			{
				case '_onPing' :
					this._onPing( message );
					break;
				case '_onStore' :
					this._onStore( message );
					break;
				case '_onStoreReply' :
					this._onStoreReply( message );
					break;
				case '_onFindValue' :
					this._onFindValue( message );
					break;
				case '_onFindNode' :
					this._onFindNode( message );
					break;
				default:
					_.bind( pfnAction, this )( message );
					break;
			}

		}
		else
		{
			console.warn( "Unknown message", message );
		}
	}


	_onPing( message )
	{
		// this can be made more intelligent such that
		// if an outgoing message is present, piggyback the pong
		// onto it rather than sending it separately
		this._rpc.send
		(
			MC( message ),
			this._MSG( 'PONG', { 'replyTo' : MID( message ) } )
		);
	}

	_updateContact( contact, cb )
	{
		if ( ! contact )
			return;

		let callback = cb || function() {};
		let bucketIndex = util.bucketIndex(this.self.nodeID, contact.nodeID);

		//	...
		assert.ok( bucketIndex < constants.B );

		if ( ! this._buckets[ bucketIndex ] )
			this._buckets[ bucketIndex ] = new Bucket();

		let bucket = this._buckets[bucketIndex];
		contact.lastSeen = Date.now();

		let exists = bucket.contains(contact);
		if ( exists )
		{
			// move to the end of the bucket
			bucket.remove(contact);
			bucket.add(contact);
			callback();
		}
		else if ( bucket.size() < constants.K )
		{
			bucket.add(contact);
			callback();
		}
		else
		{
			this._rpc.send
			(
				bucket.get( 0 ),
				this._MSG( 'PING' ),
				_.bind
				(
					function( err, message )
					{
						if ( err )
						{
							//	add new contact, old one is dead
							bucket.removeIndex( 0 );
							bucket.add( contact );
						}
						else
						{
						}

						callback();
					},
					this
				)
			);
		}
	}

	/**
	 *	@param message
	 *	@private
	 *
	 *	TODO: handle large values which
	 *	won't fit in single UDP packets
	 */
	_onStore( message )
	{
		if ( ! message.key || message.key.length !== constants.B / 4 )
			return;
		if ( ! message.value )
			return;

		this._storage[ message.key ] = _.clone( message.value );
		this._rpc.send
		(
			MC( message ),
			this._MSG
			(
				'STORE_REPLY',
				{
					'replyTo': MID(message),
					'status': true
				}
			)
		);
	}

	/**
	 *	This is just to prevent Unknown message errors
	 *	@private
	 */
	_onStoreReply()
	{
		return;
	}

	_onFindValue( message )
	{
		if ( ! message.key || message.key.length !== constants.B / 4 )
			return;

		if ( this._storage.hasOwnProperty(message.key ) )
		{
			this._rpc.send
			(
				MC(message),
				this._MSG
				(
					'FIND_VALUE_REPLY',
					{
						'replyTo': MID(message),
						'found': true,
						'value': this._storage[message.key]
					}
				)
			);
		}
		else
		{
			let messageContact = MC(message);
			if ( ! messageContact )
				return;

			let contacts = this._findClosestNodes( message.key, constants.K, MC(message).nodeID );

			this._rpc.send
			(
				MC( message ),
				this._MSG
				(
					'FIND_NODE_REPLY',
					{
						'replyTo': MID(message),
						'contacts': contacts
					}
				)
			);
		}
	}

	_findClosestNodes( key, howMany, exclude )
	{
		let contacts = [];
		function addContact(contact)
		{
			if ( ! contact )
				return;

			if ( contacts.length >= howMany )
				return;

			if ( contact.nodeID === exclude )
				return;

			contacts.push( contact );
		}

		function addClosestFromBucket( bucket )
		{
			let distances = _.map( bucket.contacts(), function( contact )
			{
				return {
					distance: util.distance(contact.nodeID, key),
					contact: contact
				};
			});

			distances.sort( function( a, b )
			{
				return util.buffer_compare( a.distance, b.distance );
			});

			_(distances).chain()
			.first( howMany - contacts.length )
			.pluck( 'contact' )
			.map( MC )
			.value()
			.forEach( addContact );
		}

		//
		//	first check the same bucket
		//	what bucket would key go into, that is the closest
		//	bucket we start from, hence bucketIndex
		//	is with reference to self.nodeID
		//
		let bucketIndex = util.bucketIndex( this.self.nodeID, key );
		if ( this._buckets.hasOwnProperty( bucketIndex ) )
		{
			addClosestFromBucket( this._buckets[ bucketIndex ] );
		}

		let oldBucketIndex = bucketIndex;

		//	then check buckets higher up
		while ( contacts.length < howMany && bucketIndex < constants.B )
		{
			bucketIndex ++;
			if ( this._buckets.hasOwnProperty( bucketIndex ) )
			{
				addClosestFromBucket( this._buckets[ bucketIndex ] );
			}
		}

		//
		//	then check buckets lower down
		//	since Kademlia does not define the search strategy, we can cheat
		//	and use this strategy although it may not actually return the closest
		//	FIXME: be intelligent to choose actual closest
		//
		bucketIndex = oldBucketIndex;
		while ( contacts.length < howMany && bucketIndex >= 0 )
		{
			bucketIndex--;
			if ( this._buckets.hasOwnProperty( bucketIndex ) )
			{
				addClosestFromBucket( this._buckets[ bucketIndex ] );
			}
		}

		return contacts;
	}

	_refreshBucket( bucketIndex, callback )
	{
		let random = util.randomInBucketRangeBuffer( bucketIndex );
		this._iterativeFindNode( random.toString( 'hex' ), callback );
	}

	// this is a primitive operation, no network activity allowed
	_onFindNode( message )
	{
		if ( ! message.key || message.key.length !== constants.B/4 || ! MC( message ) )
		{
			return;
		}

		let contacts = this._findClosestNodes( message.key, constants.K, MC( message ).nodeID );

		this._rpc.send
		(
			MC( message ),
			this._MSG
			(
				'FIND_NODE_REPLY',
				{
					'replyTo'	: MID( message ),
					'contacts'	: contacts
				}
			)
		);
	}

	//
	//	cb should be function(err, type, result)
	//	where type == 'VALUE' -> result is the value
	//		type == 'NODE'  -> result is [list of contacts]
	//
	_iterativeFind( key, mode, cb )
	{
		assert.ok( _.include( [ 'NODE', 'VALUE' ], mode ) );

		//	...
		let __this 			= this.m_oThis;
		let externalCallback		= cb || function() {};
		let closestNode			= null;
		let previousClosestNode 	= null;
		let closestNodeDistance 	= -1;
		let shortlist			= this._findClosestNodes( key, constants.ALPHA, this.self.nodeID );
		let contacted			= {};
		let foundValue			= false;
		let value			= null;
		let contactsWithoutValue	= [];

		//	nodes list exclude self
		closestNode	= shortlist[ 0 ];
		if ( ! closestNode )
		{
			//	we aren't connected to the overlay network!
			externalCallback
			(
				{ message: 'Not connected to overlay network. No peers.' },
				mode,
				null
			);
			return;
		}

		//	...
		closestNodeDistance	= util.distance( key, closestNode.nodeID );

		function xyz( alphaContacts )
		{
			//
			//	clone because we're going to be modifying inside
			//
			async.forEach
			(
				alphaContacts,
				function( contact, callback )
				{
					//	...
					__this._rpc.send
					(
						contact,
						__this._MSG
						(
							'FIND_' + mode,
							{
								key : key
							}
						),
						function( err, message )
						{
							if ( err )
							{
								//
								//	occurred an error
								//
								console.log( "ERROR in iterativeFind" + _s( '_' + mode ).camelize() + " send to", contact );
								shortlist = _.reject
								(
									shortlist,
									function( el )
									{
										return el.nodeID === contact.nodeID;
									}
								);
							}
							else
							{
								__this._updateContact( contact );
								contacted[ contact.nodeID ] = true;

								let dist = util.distance( key, contact.nodeID );

								if ( util.buffer_compare( dist, closestNodeDistance ) === -1 )
								{
									previousClosestNode	= closestNode;
									closestNode		= contact;
									closestNodeDistance	= dist;
								}

								//
								//	found
								//
								if ( message.found && mode === 'VALUE' )
								{
									foundValue	= true;
									value		= message.value;
								}
								else
								{
									if ( mode === 'VALUE' )
									{
										//	not found, so add this contact
										contactsWithoutValue.push( contact );
									}

									//	...
									shortlist	= shortlist.concat(message.contacts);
									shortlist	= _.uniq( shortlist, false /* is sorted? */, function( contact )
									{
										return contact.nodeID;
									});
								}
							}

							//	...
							callback();

						}
					);
				},
				function( err )
				{
					if ( foundValue )
					{
						let thisNodeID	= __this.self.nodeID;
						let distances	= _.map( contactsWithoutValue, function( contact )
						{
							return {
								distance: util.distance(contact.nodeID, thisNodeID),
								contact: contact
							};
						});

						distances.sort( function( a, b ) {
							return util.buffer_compare( a.distance, b.distance );
						});

						if ( distances.length >= 1 )
						{
							let closestWithoutValue = distances[0].contact;

							console.log( "Closest is ", closestWithoutValue );
							let message = __this._MSG
							(
								'STORE',
								{
									'key': key,
									'value': value
								}
							);

							__this._rpc.send( closestWithoutValue, message );
						}

						//	...
						externalCallback( null, 'VALUE', value );
						return;
					}

					if ( closestNode === previousClosestNode || shortlist.length >= constants.K )
					{
						//
						//	TODO: do a FIND_* call on all nodes in shortlist
						//	who have not been contacted
						//
						externalCallback( null, 'NODE', shortlist );
						return;
					}

					//	...
					let nRemain	= _.reject( shortlist, function( el ) { return contacted[ el.nodeID ]; } );
					if ( nRemain.length === 0 )
					{
						externalCallback( null, 'NODE', shortlist );
					}
					else
					{
						xyz( _.first( nRemain, constants.ALPHA ) );
					}

				}
			);
		}

		//	...
		xyz( shortlist );
	}

	_iterativeFindNode( nodeID, cb )
	{
		this._iterativeFind( nodeID, 'NODE', cb );
	}

	//
	//	cb -> function(err, value)
	//	this does not map over directly to the spec
	//	rather iterativeFind already does the related things
	//	if the callback gets a list of contacts, it simply
	//	assumes the key does not exist in the DHT (atleast with
	//	available knowledge)
	//
	_iterativeFindValue( key, cb )
	{
		let callback = cb || function() {};

		//	...
		this._iterativeFind( key, 'VALUE', function( err, type, result )
		{
			if ( type === 'VALUE' )
			{
				callback( null, result );
			}
			else
			{
				callback
				(
					{
						'code': 'NOTFOUND',
						'key': key
					},
					null
				);
			}
		});
	}

	toString()
	{
		return "Node " + this.self.nodeID + ":" + this.self.address + ":" + this.self.port;
	}

	debug()
	{
		console.log(this.toString());
		_(this._buckets).each(function(bucket, j) {
			console.log("bucket", j, bucket.toString());
		});
		console.log("store", this._storage);
	}

	/**
	 *	@public
	 *
	 *	@param address
	 *	@param port
	 *	@param cb
	 */
	connect( address, port, cb )
	{
		let callback	= cb || function() {};

		//	...
		assert.ok( this.self.nodeID );
		let contact	= util.make_contact( address, port );

		let refreshBucketsFartherThanClosestKnown = function( type, contacts, asyncCallback )
		{
			//	FIXME: Do we update buckets or does iterativeFindNode do it?
			let leastBucket		= _.min( _.keys( this._buckets ) );
			let bucketsToRefresh	= _.filter( _.keys( this._buckets ), function( num ) { return num >= leastBucket; });
			let queue		= async.queue( _.bind( this._refreshBucket, this ), 1 );
			_.each(bucketsToRefresh, function(bucketId) {
				// wrapper is required because the each iterator is passed
				// 3 arguments (element, index, list) and queue.push interprets
				// the second argument as a callback
				queue.push(bucketId);
			});
			asyncCallback(); // success
		}

		async.waterfall([
			_.bind(this._updateContact, this, contact), // callback is invoked with no arguments
			_.bind(this._iterativeFindNode, this, this.self.nodeID), // callback is invoked with
										 // type (NODE) and shortlist,
										 // which gets passed on to
										 // refreshBucketsFartherThanClosestKnown
			_.bind(refreshBucketsFartherThanClosestKnown, this) // callback is invoked with no arguments
		], callback);
	}

	get( key, cb )
	{
		let callback = cb || function() {};
		this._iterativeFindValue( util.id( key ), callback );
	}

	set( key, value, cb )
	{
		let callback = cb || function() {};
		let message = this._MSG('STORE', {
			'key': util.id(key),
			'value': value
		});
		this._iterativeFindNode(util.id(key), _.bind(function(err, type, contacts) {
			if (err) {
				callback(err);
				return;
			}
			async.forEach(contacts, _.bind(function(contact, asyncCb) {
				this._rpc.send(contact, message, function() {
					// TODO handle error
					asyncCb(null);
				});
			}, this), callback);
		}, this));
	}
}




/**
 *	exports
 */
exports.KNode	= KNode;