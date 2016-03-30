// nosql-lab-dataimport -- to redis :-)
"use strict";

var postsReader  = require('./read-posts');
var Redis       = require('ioredis');

/* We're using ioredis, the currently recommended fast client
 * check https://github.com/luin/ioredis for details
 */
//var redis = new Redis(6379, "127.0.0.1", {
   // var redis = new Redis(8080, "10.23.232.107", {
 //var redis = new Redis(21, "ctcvw5009", {      //22.6 sec
 var redis = new Redis(22, "10.23.232.155", {
  showFriendlyErrorStack: true,
  retryStrategy: function (times) {
    if(times <= 5) {
      console.warn('Redis: no connection, retry #%d of 5', times);
      return 500;
    }
    console.error('Redis: no connection, giving up');
    return "";
  }
}); 

let filePosts = __dirname + '/raw_xml/Posts.xml';
let countPosts = 0;
let counter = 0;
console.log('Processing posts from "%s"', filePosts);
postsReader.readPosts(__dirname + '/raw_xml/Posts.xml', function *() {
  let elem = yield;
  
  while(elem) {
    if(elem instanceof postsReader.Post) {
      let meta = { 'type': 'post', 'title': elem.title, 'creationDate': elem.creationDate };      
      redis.hset("myhash",elem.postId + '.meta', JSON.stringify(meta));
      redis.hset("myhash",elem.postId + '.body', elem.body);
    } else {
      // response!
      let meta = { 'type': 'response', 'parentId': elem.parentId, 'creationDate': elem.creationDate };      
      redis.hset("myhash",elem.responseId + '.meta', JSON.stringify(meta));
      redis.hset("myhash",elem.responseId + '.body', elem.body);
    }
    


 
   /*
    switch(countPosts % 500) {
     case 0:
        process.stdout.write('-');
        break;        
     case 2:
        process.stdout.write('\\');
        break;
     case 3:
        process.stdout.write('|');
        break;
     case 4:
        process.stdout.write('/');
     default:
 } 
 counter++;
 
  /*  
    if((countPosts % 50)==0) {
      process.stdout.write('/');
    }
    */
    countPosts++;
    
    // get next entry
    elem = yield;
  }
  process.stdout.write('\n');
  console.log('We are done. uploaded ='+ (countPosts * 2));
  redis.quit();
});
