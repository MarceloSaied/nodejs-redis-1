"use strict";

var fs          = require('fs');
var sax         = require('sax');
var saxpath     = require('saxpath');
var parseString = require('xml2js').parseString;

class Post {
  constructor(xml) {
    this.id = parseInt(xml.Id);
    this.postId = parseInt(xml.Id);
    this.typeId = parseInt(xml.PostTypeId);
    this.title = String(xml.Title);
    this.creationDate = String(xml.CreationDate);
    this.body = String(xml.Body);
    this.score = parseInt(xml.Score);
    this.owner = parseInt(xml.OwnerUserId);
    this.lastEditor = parseInt(xml.LastEditorUserId);
    this.lastActivityDate = parseInt(xml.LastActivityDate);
    this.commentCount = parseInt(xml.CommentCount);
  }
}

// Response is just like Post, without Title but with parentID
class Response {
  constructor(xml) {
    this.id = parseInt(xml.Id);
    this.responseId = parseInt(xml.Id);
    this.typeId = parseInt(xml.PostTypeId);
    this.parentId = parseInt(xml.ParentId);
    this.creationDate = String(xml.CreationDate);
    this.body = String(xml.Body);
    this.score = parseInt(xml.Score);
    this.owner = parseInt(xml.OwnerUserId);
    this.lastEditor = parseInt(xml.LastEditorUserId);
  }
}

function readPosts(filename, generator) {
  let fileStream = fs.createReadStream(filename, {encoding: 'utf-8'});
  let saxParser  = sax.createStream(true, {trim: true, strictEntities: true});
  let streamer = new saxpath.SaXPath(saxParser, '/posts/row');
	let it = generator();
  
  streamer.on('match', function(xml) {
    // Ok we have a post -- now let's be lazy and convert it :-)
    parseString(xml, function(err, result) {
      let typeID = parseInt(result.row.$.PostTypeId);
      
      // create adequate type and pass out to yield()
      if(typeID === 1) {
        let post = new Post(result.row.$);
        it.next(post);
      } else {
        let response = new Response(result.row.$);
        it.next(response);
      }
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

exports.Post = Post;
exports.Response = Response;
exports.readPosts = readPosts;
