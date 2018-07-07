"use strict";

let KNode	= require( '../lib/knode' ).KNode;
let _node	= new KNode( { address: '127.0.0.1', port: 1214 } );


setInterval
(
	() =>
	{
		let nTick;

		nTick	= Date.now();
		_node.set( 'tick', nTick, function( err )
		{
			if ( err )
			{
				console.error( '#' + ( new Date() ).toLocaleString() + ' | node.set tick :: ', err );
			}
			else
			{
				console.log( '$' + ( new Date() ).toLocaleString() + ' | node.set tick as ' + nTick + ' :: successfully' );
			}

		});
	},
	10 * 1000
);


