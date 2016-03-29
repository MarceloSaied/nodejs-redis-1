// nosql-lab-dataimport -- to redis :-)
"use strict";

var readPosts   = require('./read-posts').readPosts;
var assert      = require('assert');
var MongoClient = require('mongodb').MongoClient;
var ObjectId    = require('mongodb').ObjectID;

var url = 'mongodb://localhost:27017/nosqllab';
// Use connect method to connect to the Server
MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected correctly to mongodb server");
  let collection = db.collection('posts');
  
  let filePosts = __dirname + '/raw_xml/Posts.xml';
  let countPosts = 0;
  let countResponses = 0;
  console.log('Processing posts from "%s"', filePosts);
  readPosts(__dirname + '/raw_xml/Posts.xml', function *() {
    let post = yield;
    
    while(post != null) {
      // postType 1 is for real questions (and not answers)
      if(post.typeId === 1) {
        collection.insertOne({
          _id: post.id,     // match users-collection uniqueID to source
          title: post.title,
          creationDate: new Date(post.creationDate), // store a real date :-)
          body: post.body,
          score: post.score,
          owner: post.owner,
          lastEditor: post.lastEditor
        }, {w: 0}, function(err, result) {
          assert.equal(err, null);
        });
  
        countPosts++;
      } else {
        // We have a response :-)
        
        collection.updateOne(
          { _id: post.parentId },   // match users-collection uniqueID to source
          { $push: { responses: {            
            id: post.id,
            typeId: post.typeId,    // could be removed
            parentId: post.parentId,       // unnecessary...
            creationDate: new Date(post.creationDate), // store a real date :-)
            body: post.body,
            score: post.score,
            owner: post.owner,
            lastEditor: post.lastEditor
          }}  
        }, {w: 0}, function(err, result) {          
          assert.equal(err, null);
        });
        countResponses++;
      }
      
      if(((countPosts + countResponses) % 100)==0) {
        process.stdout.write('.');
      }
      
      // get next entry
      post = yield;
    }
    process.stdout.write('\n');
    console.log('Processed %d posts and %d responses', countPosts, countResponses);
    
 // Initilize the sequence for Post Ids   
    db.collection('counters').insertOne(
      { _id: "posts", sequence: 27515 }
    );
    
    console.log('We are done');
    db.close();
  });
});
