// nosql-lab-dataimport -- to redis :-)
"use strict";

var postsReader    = require('./read-posts');
var usersReader    = require('./read-users');
var commentsReader = require('./read-comments');
var assert         = require('assert');
var async          = require('async');
var neo            = require('seraph')({
  user: 'neo4j',
  pass: 'nosqllab'
});

console.log('!!! YOU MUST ENSURE THE NEO4J DATABASE IS EMPTY !!!');
console.log();
console.log('If not, you MUST stop neo, DELETE the graph.db folder and RESTART');
console.log('Reasoning: Uniqueness constraint combined with batch-import lead to colissions we cannot nicely recover from');
console.log();

// XML's userId -> neo-internal-id
let userMapping = new Map();
// XML's postId -> neo-internal-id
let postMapping = new Map();
// XML's responseId -> neo-internal-id
let responseMapping = new Map();
// XML's commentId -> neo-internal-id
let commentMapping = new Map();

// Work Maps:
let responds_to = new Map();  // responseId -> postId
let post_owner  = new Map();  // postId -> userId
let response_owner = new Map(); // responseId -> userId
let comment_post = new Map(); // commentId -> postId
let comment_user = new Map(); // commentId -> userId

// neo4j fails miserably on curly braces (!!)
// in perfectly valid strings. The latter is due to a bug handling the referencing
// mechanisms in the batch-API. The dataset contains this stuff (check postid 18307)
// due to Math-expressions in the body :-)
// filed a bug report: https://github.com/neo4j/neo4j/issues/5664
function escapeBraces(unsafe) {
    return unsafe.replace(/[\{\}]/g, function (c) {
        switch (c) {
            case '{': return '&#123;';
            case '}': return '&#125;';
        }
    });
}

function toNeo(post) {
  let result = {
    creationDate: post.creationDate,
    body: escapeBraces(post.body),
    score: post.score
  };
  
  if(post instanceof postsReader.Post) {
    result.postId = post.postId;
    result.title = post.title;       
  } else {
    result.responseId = post.responseId;
    
    if(Number.isFinite(post.parentId))
      result.parentId = post.parentId;
  }
  
  if(Number.isFinite(post.owner))
    result.owner = post.owner;
  
  if(Number.isFinite(post.lastEditor)) 
    result.lastEditor = post.lastEditor;
  
  return result;
}

// globally usable progress mechanism
let count = 0;
function progress(batchsize) {
  count++;
  if((count % batchsize) === 0) {
    process.stdout.write('.');  
  }
}

async.series([
  callback => {
    console.log('Ensuring uniqueness for Users (label user, field userId)');
    neo.constraints.uniqueness.createIfNone('user', 'userId', function(err, constraint) {
      if(err) throw err;
      console.log(constraint);
      callback();
    });
  },
  callback => {
    console.log('Ensuring uniqueness for Posts (label post, field postId)');
    neo.constraints.uniqueness.createIfNone('post', 'postId', function(err, constraint) {
      if(err) throw err;
      console.log(constraint);
      callback();
    });
  },
  callback => {
    console.log('Ensuring uniqueness for Responses (label response, field responseId)');
    neo.constraints.uniqueness.createIfNone('response', 'responseId', function(err, constraint) {
      if(err) throw err;
      console.log(constraint);
      callback();
    });
  },
  callback => {
    console.log('Ensuring uniqueness for Comments (label comment, field commentId)');
    neo.constraints.uniqueness.createIfNone('comment', 'commentId', function(err, constraint) {
      if(err) throw err;
      console.log(constraint);
      callback();
    });
  },
  /* **************** USER CREATION ******************************** */
  callback => {
    let filename = __dirname + '/raw_xml/Users.xml';
    let batchsize = 500;
    count = 0;
    console.log('Processing users from "%s", batchsize %d', filename, batchsize);
    
    let processor = async.cargo(function(tasks, cbCargo) {
      neo.batch(function(txn) {
        for(let elem of tasks) {
          let node = txn.save(elem);
          txn.label(node, 'user');
        }
      }, function(err, results) {
        if(err) {
          console.log(err);
          console.log('could not save ', results); 
        } else {
          for(let elem of results) {
            if(elem && elem.userId) {
              userMapping.set(elem.userId, elem.id);     
              progress(batchsize);
            }
          }
        }
        cbCargo(err);
      });
    }, batchsize);
    
    processor.drain = function() {
      process.stdout.write('\n');
      console.log('Successfully processed %d users', userMapping.size);
      callback();
    }

    usersReader.readUsers(filename, function *() {
      let post = yield;
      
      while(post != null) {
        processor.push(post);
        
        // get next entry
        post = yield;
      }
    });    
  },
  /* **************** POST & RESPONSE CREATION ********************* */
  callback => {
    let fileName = __dirname + '/raw_xml/Posts.xml';
    let batchsize = 500;
    count = 0;
    console.log('Processing posts from "%s", batchsize %d', fileName, batchsize);
    
    let processor = async.cargo(function(tasks, cbCargo) {
      neo.batch(function(txn) {
        for(let elem of tasks) {
          let node = txn.save(toNeo(elem));
          if(elem instanceof postsReader.Post) {
            txn.label(node, 'post');
          } else {
            txn.label(node, 'response');
          }
        }
      }, function(err, results) {
        if(err) {
          console.log(err);
          console.log('could not save ', results); 
        } else {
          for(let elem of results) {  
            // Stupidly we don't get the labels included in response objects :-(          
            if(elem && elem.postId) {
              postMapping.set(elem.postId, elem.id);
              /*
              if(elem.postId === 27355)
                console.log(elem, postMapping.get(elem.postId));
              */
              post_owner.set(elem.postId, elem.owner); // everything has an owner :-)
              progress(batchsize);
            } else if(elem && elem.responseId) {
              responseMapping.set(elem.responseId, elem.id);
              response_owner.set(elem.responseId, elem.owner);
              if(elem.parentId)
                responds_to.set(elem.responseId, elem.parentId);
              progress(batchsize);
            }
          }
        }
        cbCargo(err);
      });
    }, batchsize);  // neo has to batches of 500 :-)
    
    processor.drain = function() {
      process.stdout.write('\n');
      console.log('Successfully processed %d posts and %d responses', 
        postMapping.size, responseMapping.size);
      callback();
    }

    postsReader.readPosts(fileName, function *() {
      let post = yield;
      
      while(post != null) {
        processor.push(post);
        
        // get next entry
        post = yield;
      }
    });
  },
  /* ******************* COMMENTS CREATION ******************* */
  callback => {
    let filename = __dirname + '/raw_xml/Comments.xml';
    let batchsize = 500;
    count = 0;
    console.log('Processing comments from "%s", batchsize %d', filename, batchsize);
    
    let processor = async.cargo(function(tasks, cbCargo) {
      neo.batch(function(txn) {
        for(let elem of tasks) {
          elem.text = escapeBraces(elem.text);
          let node = txn.save(elem);
          txn.label(node, 'comment');
        }
      }, function(err, results) {
        if(err) {
          console.log(err);
          console.log('could not save ', results); 
        } else {
          for(let elem of results) {
            if(elem && elem.commentId) {

              // let's update the maps:
              commentMapping.set(elem.commentId, elem.id);    // XML's commentId -> neo-internal-id
              comment_post.set(elem.commentId, elem.postId);  // commentId -> postId
              if(elem.userId) // yes there are comments with just a userdisplayname (ignored here)
                comment_user.set(elem.commentId, elem.userId);  // commentId -> userId
              progress(batchsize);
            }
          }
        }
        cbCargo(err);
      });
    }, batchsize);
    
    processor.drain = function() {
      process.stdout.write('\n');
      console.log('Successfully processed %d comments', userMapping.size);
      callback();
    }

    commentsReader.readComments(filename, function *() {
      let post = yield;
      
      while(post != null) {
        processor.push(post);
        
        // get next entry
        post = yield;
      }
    });    
  },
  /* **************** RELATIONS CREATION ********************* */
  callback => {
    let batchsize = 500;
    count = 0;
    console.log('Working on relations-backlog to create:');
    console.log('  %d response -- responds_to --> post', responds_to.size);
    console.log('  %d comments -- responds_to --> post', comment_post.size);
    console.log('  %d user     -- owns ---------> post', post_owner.size);
    console.log('  %d user     -- owns ---------> response', response_owner.size);
    console.log('  %d user     -- owns ---------> comment', comment_user.size);
    console.log('Ingesting with batchsize %d', batchsize);

    // lets go batch here, I messed it up with the posts and so...
    let processor = async.cargo(function(tasks, cbCargo) {
      neo.batch(function(txn) {
        for(let elem of tasks) {
          txn.relate(elem.source, elem.rel, elem.target);
        }
      }, function(err, results) {
        if(err) {
          console.error('Failed creating relationships', results, err);
        }
        progress(1);
        cbCargo(err);
      });
    }, batchsize);
    
    processor.drain = function() {
      console.log('\nRelationships Done :-)'); 
      callback();
    }

    // forEach returns value, key (!!) in that order...
    responds_to.forEach(function(postId, responseId) { // responseId -> postId
      if(postMapping.has(postId) && responseMapping.has(responseId)) {   
        processor.push({
          source: responseMapping.get(responseId),
          rel:    'responds_to',
          target: postMapping.get(postId)           
        });
      }
    }); // forEach
    responds_to.clear();
    
    comment_post.forEach(function(postId, commentId) { // commentId -> postId
      if(postMapping.has(postId) && commentMapping.has(commentId)) {
        processor.push({
          source: commentMapping.get(commentId),
          rel:    'responds_to',
          target: postMapping.get(postId)           
        });
      }
    }); // forEach
    comment_post.clear();
        
    // forEach returns value, key (!!) in that order...
    post_owner.forEach(function(userId, postId) { // postId -> userId
      if(postMapping.has(postId) && userMapping.has(userId)) {   
        processor.push({
          source: userMapping.get(userId),
          rel:    'owns',
          target: postMapping.get(postId)           
        });
      }
    }); // forEach
    post_owner.clear();
        
    response_owner.forEach(function(userId, responseId) { // responseId -> userId
      if(responseMapping.has(responseId) && userMapping.has(userId)) {   
        processor.push({
          source: userMapping.get(userId),
          rel:    'owns',
          target: responseMapping.get(responseId)           
        });
      }
    }); // forEach
    response_owner.clear();
        
    comment_user.forEach(function(userId, commentId) { // commentId -> userId
      if(commentMapping.has(commentId) && userMapping.has(userId)) {   
        processor.push({
          source: userMapping.get(userId),
          rel:    'owns',
          target: commentMapping.get(commentId)           
        });
      }
    }); // forEach
    comment_user.clear();
  },
  callback => {
    console.log('Done');
    callback();
  }
]);
