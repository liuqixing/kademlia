"use strict";

let KNode	= require( '../lib/knode' ).KNode;
let _node	= new KNode( { address: '127.0.0.1', port: 1216 } );

_node.connect( '127.0.0.1', 1214, function( err )
{
	if ( err )
	{
		console.error( 'node.connect :: ', err );
	}
	else
	{
		console.log( 'node.connect :: successfully ' );

		_node.set( 'tick', 'MaxCompute', function( err )
		{
			if ( err )
			{
				console.error( '#' + ( new Date() ).toLocaleString() + ' | node.set tick :: ', err );
			}
			else
			{
				console.log( '$' + ( new Date() ).toLocaleString() + ' | node.set tick as MaxCompute :: successfully' );
			}

		});

		setInterval
		(
			() =>
			{
				_node.get( 'tick', function( err, vValue )
				{
					if ( err )
					{
						console.error( '#' + ( new Date() ).toLocaleString() + ' node.get tick :: ', err );
					}
					else
					{
						console.log( '$' + ( new Date() ).toLocaleString() + ' node.get tick :: successfully, value is ', vValue );
					}

				});
			},
			5000
		);
	}

});