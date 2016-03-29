// nosql-lab-dataimport -- to cassandra :-)
"use strict";

var readPosts = require('./read-posts').readPosts;

var cassandra = require('cassandra-driver');
var async = require('async');

// could use cassandra bulk loaded by converting xml into csv

var client = new cassandra.Client({
	contactPoints : [ '127.0.0.1' ],
	keyspace : 'nosqllab'
});

// Tests if table exists
client.execute('SELECT * FROM post', [],

function(err, result) {

	if (err) {
		 console.log('execute failed', err.message);
		if(err.code==8704){
			console.log('Please setup tables before importing data. See lab instructions provided.');
			process.exit(0);
		}
	} else {


		let filePosts = __dirname + '/raw_xml/Posts.xml';
		let countPosts = 0;
		console.log('Processing posts from "%s"', filePosts);
		readPosts(__dirname + '/raw_xml/Posts.xml', function *() {
		  let post = yield;
		  
		  while(post != null) {
		    // postType 1 is for real questions (and not answers)
	    	
		    var query = null;
		    
		    // Quotes with in the text can mess with insert. We have seen this in RDBMS as well.
		    post.body = post.body.trim().replace(/'/g, '&#39;');
	    	var created = Date.parse(post.creationDate);
	    	var creationDate = post.creationDate.substring(0,10);
	    	var lastActivityDate = Date.parse(post.lastActivityDate);
	    	
	    	if(!post.lastEditor) post.lastEditor = 0;
	    	if(!post.owner) post.owner = 0;
	    	
		    if(post.typeId==1){
		    	
			// console.log("Adding POST");
		    	
		    	var one_hour = 3 * 60 * 60;
		    	var created_hash = created%one_hour;
		    	
		    	if(post.title) post.title = post.title.replace(/'/g, '&#39;');
		    	
			    query = "INSERT INTO post (id, title, body, created, score, lastActivityDate, commentCount, "
						+ " ownerUserId, lastEditorUserId, creationDate, created_hash) "
			    	+"VALUES ("+post.id+", '"+ post.title +"', '"+ post.body +"', "+created+", "+post.score+", "+lastActivityDate+", "+post.commentCount
			    	+", "+post.owner+", "+post.lastEditor+", '"+creationDate+"', '"+created_hash+"')";
			    
			    send_query(query, countPosts);
			    // console.log(query);
		    } else {
		    	
		    	if(!post.parentId) post.parentId = 0;
		    	
		    	 query = "INSERT INTO response (id, body, created, score, parentId, ownerUserId, lastEditorUserId) "
			    	+"VALUES ("+post.id+", '"+ post.body +"', "+created+", "+post.score+", "+post.parentId+", "+post.owner
			    	+", "+post.lastEditor+")";
	    		 
	    		 // console.log(query);
	    		 send_query(query, 0);
		    	
		    }
	    	
		    
		    // get next entry
		    post = yield;
		  }
		  process.stdout.write('\n');
		  console.log('We are done');
		});
	}

});

function send_query(query, countPosts){
	client.execute(query, [],
			
			function(err, result) {

				if (err) {
					console.log('execute failed', err);
					process.exit(0);
//				} else {
//					 console.log(query);
				}
			});
}

function errorHandler(err, operation, end) {
	if (err) {
		console.log(operation + ' failed', err);
		if(end) process.exit(0);
	} else {
		console.log(operation + ' succeeded.');
	}
}

