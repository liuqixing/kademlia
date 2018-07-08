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

let assert		= require( 'assert' );
let node_constants	= require( 'constants' );
let dgram		= require( 'dgram' );
let _			= require( 'underscore' );
let hat			= require( 'hat' );
let constants		= require( './constants' );


class RPC
{
	constructor( bindAddress, callback )
	{
		this._socket		= dgram.createSocket( 'udp4', _.bind( this._onMessage, this ) );
		this._socket.bind( bindAddress.port, bindAddress.address || undefined );
		this._callback		= callback;
		this._rack		= hat.rack( constants.B );
		this._awaitingReply	= {};

		//	every node requires only one of these this way
		setInterval
		(
			_.bind( this._expireRPCs, this ),
			constants.T_RESPONSETIMEOUT + 5
		);
	}

	send( node, message, callback )
	{
		if ( ! node )
			return;

		assert( node.port );
		assert( node.address );

		_.extend( message, { rpcID: this._rack() } );

		let data = new Buffer(JSON.stringify(message), 'utf8');
		this._socket.send( data, 0, data.length, node.port, node.address );
		if ( _.isFunction( callback ) )
		{
			//	only store rpcID if we are expecting a reply
			this._awaitingReply[ message.rpcID ] = { timestamp: Date.now(), callback: callback };
		}
	}

	close()
	{
		this._socket.close();
	}

	_onMessage( data, rinfo )
	{
		let message;
		try {
			message = JSON.parse( data.toString( 'utf8' ) );
		}
		catch ( e )
		{
			/* we simply drop the message, although this
			 * reduces the reliability of the overall network,
			 * we really don't want to implement a reliable
			 * network over UDP until it is really required.
			 */
			return;
		}

		// console.log( '============================================================' );
		// console.log( 'RPC::RECEIVE ', message );
		// console.log( '============================================================' );

		if ( message.replyTo && this._awaitingReply.hasOwnProperty( message.replyTo ) )
		{
			let cb	= this._awaitingReply[ message.replyTo ].callback;
			delete this._awaitingReply[ message.replyTo ];
			cb( null, message );
			return;
		}

		this._callback( message );
	}

	_expireRPCs()
	{
		let __this	= this;
		let now		= Date.now();
		let discarded	= 0;

		//	...
		_.keys( this._awaitingReply ).forEach
		(
			function( k )
			{
				if ( now - __this._awaitingReply[ k ].timestamp > constants.T_RESPONSETIMEOUT )
				{
					__this._awaitingReply[ k ].callback
					({
						errno	: node_constants.ETIMEDOUT,
						code	: 'ETIMEDOUT',
						rpcID	: k
					}, null );

					//	...
					delete __this._awaitingReply[ k ];
					discarded ++;
				}
			}
		);
		//if (discarded)
		//    console.warn("expireRPCs: discarded %d since no reply was received", discarded);
	}
}




/**
 *	exports
 */
exports.RPC	= RPC;

