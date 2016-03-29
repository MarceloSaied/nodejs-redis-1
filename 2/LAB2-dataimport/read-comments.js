"use strict";

var fs          = require('fs');
var sax         = require('sax');
var saxpath     = require('saxpath');
var parseString = require('xml2js').parseString;

class Comment {
  constructor(xml) {
    this.commentId = parseInt(xml.Id);
    this.postId = parseInt(xml.PostId);
    this.text = String(xml.Text);
    this.creationDate = String(xml.CreationDate);
    this.score = parseInt(xml.Score);
    if(xml.UserId) this.userId = parseInt(xml.UserId);
  }
}

function readComments(filename, generator) {
  let fileStream = fs.createReadStream(filename, {encoding: 'utf-8'});
  let saxParser  = sax.createStream(true, {trim: true, strictEntities: true});
  let streamer = new saxpath.SaXPath(saxParser, '/comments/row');
	let it = generator();
  
  streamer.on('match', function(xml) {
    // Ok we have a post -- now let's be lazy and convert it :-)
    parseString(xml, function(err, result) {
      // create adequate type and pass out to yield()
      let user = new Comment(result.row.$);
      it.next(user);
    });
  });
  
  streamer.on('end', function(){
    // we are done, let's yield null as a signal!
    it.next(null);
  });
  
  fileStream.pipe(saxParser);
  
  // start signal
  // because we're "reversing" the generator here, there is no
  // real back-channel for the first value, so lets skip over
  it.next();
}

exports.Comment = Comment;
exports.readComments = readComments;
