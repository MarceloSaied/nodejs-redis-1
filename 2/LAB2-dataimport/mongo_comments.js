// nosql-lab-dataimport -- to mongodb :-)
"use strict";

var fs          = require('fs');
var sax         = require('sax');
var saxpath     = require('saxpath');
var parseString = require('xml2js').parseString;
var assert      = require('assert');
var MongoClient = require('mongodb').MongoClient;
var ObjectId    = require('mongodb').ObjectID;

var url = 'mongodb://localhost:27017/nosqllab';
// Use connect method to connect to the Server
MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected correctly to mongodb server");
  let collection = db.collection('posts');

  class Comment {
    constructor(xml) {
      this.id = parseInt(xml.Id);
      this.postId = parseInt(xml.PostId);
      this.text = String(xml.Text);
      this.creationDate = String(xml.CreationDate);
      this.score = parseInt(xml.Score);
      this.userId = parseInt(xml.UserId);
    }
  }
  
  var filename = 'Comments.xml';
  console.info('Loading first file: %s ; reporting every 100 records', filename);
  
  var fileStream = fs.createReadStream(__dirname + '/raw_xml/' + filename, {encoding: 'utf-8'});
  var saxParser  = sax.createStream(true, {trim: true, strictEntities: true});
  var streamer = new saxpath.SaXPath(saxParser, '/comments/row');
  
  let count = 0;
  
  streamer.on('match', function(xml) {
    // Ok we have a comment -- now let's be lazy and convert it :-)
    parseString(xml, function(err, result) {
        let comment = new Comment(result.row.$);
        
        collection.updateOne(
          { _id: comment.postId },   // match users-collection uniqueID to source
           { $push: { comments: {           
            id: comment.id,
            postId: comment.postId,    
            text: comment.text,       
            creationDate: new Date(comment.creationDate), // store a real date :-)
            score: comment.score,
            userId: comment.userId
          }}  
        }, {w: 0}, function(err, result) {           
          assert.equal(err, null);
        });
  
        if((count % 100)==0) {
          process.stdout.write('.');
        }
        
        count++;   
      
    });
  });
  
  streamer.on('end', function(){
    process.stdout.write('\n');
    console.log('Loaded %d comments', count);
    console.log('We are done. Please note that the lowest write concern was used, hence ' +
      'it will loose data even without errors :-)');
    // close mongodb connection, this is still keeping
    // the node process alive
    db.close();
  });

  fileStream.pipe(saxParser);
});
