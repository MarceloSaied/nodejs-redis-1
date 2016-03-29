// nosql-lab-redis
"use strict";

var express = require('express');
var path    = require('path');
var logger  = require('morgan');
var Redis   = require('ioredis');

// let's get it started:
var app = express();      // init express
app.use(logger('dev'));   // URL logging

// view engine to save us from painful html typing
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'hjs');
app.use(express.static(path.join(__dirname, 'static')));

/* We're using ioredis, the currently recommended fast client
 * check https://github.com/luin/ioredis for details
 */
var redis = new Redis(6379, "127.0.0.1", {
  showFriendlyErrorStack: true,
  retryStrategy: function (times) {
    if(times <= 5) {
      console.warn('Redis: no connection, retry #%d of 5', times);
    }
    console.error('Redis: no connection, giving up');
    return 500;
  }
});

/*
 * Here on "/" we simply want to LIST all blog entries 
 * we have...
 */
app.get('/', function (req, res) {
  redis.keys('*').then(function (result) {
       
      
    // this gives us an array of key NAMES
   /*  orig  returningto the console alll the return 
    console.log('KEYS returned %j', result);
   */
  console.log('KEYS returned %j', result);
  
    res.render('index', { keys: result });
  }).catch(function (err) {
    console.log('KEYS went wrong...', err);
    res.status(500).send(err.message);
  });
});

/*
 * Here we really want to access a specific blog-post
 */
app.get('/post/:postId', function (req, res) {
  let postId = req.params.postId;
  
  redis.get(postId, function(err, result) {
    if(err) {
      console.warn('Couldn\'t get "%s"', postId, err);
      res.status(501).send('Couldn\'t retrieve element ' + err);
      return;
    }
    
    if(result) {
        //by marcelo
        let parsed_content = JSON.parse(result);
        //    
        /*  original
        res.render('post', { postId: postId, content: result });
        */
        // by marcelo parsing content for rendering
        res.render('post', {
            postId: postId,
            content: parsed_content,
            raw_content : result 
        });
        //
    } else {
      // not found as result = null
      res.status(404).send('Post ' + postId + ' not found');
    }
  });
});

redis.on('ready', function (err, redisclient) {
  // Let's boot up the server :-)
  var server = app.listen(3000, function () {
    var host = server.address().address;
    var port = server.address().port;
  
    console.log('Example app listening at http://%s:%s', host, port);
  });
});

module.exports = app;
