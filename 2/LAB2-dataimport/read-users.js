"use strict";

var fs          = require('fs');
var sax         = require('sax');
var saxpath     = require('saxpath');
var parseString = require('xml2js').parseString;

/*
		Id="2" 
		Reputation="101" 
		CreationDate="2011-07-12T16:33:06.010" 
		DisplayName="Geoff Dalgas" 
		LastAccessDate="2013-07-15T17:36:55.193" 
		WebsiteUrl="http://stackoverflow.com" 
		Location="Corvallis, OR" 
		AboutMe="Developer on the StackOverflow team. [...]" 
		Views="8" 
		UpVotes="0" 
		DownVotes="0" 
		Age="38" 
		AccountId="2" 
    */

class User {
  constructor(xml) {
    this.userId = parseInt(xml.Id);
    this.reputation = parseInt(xml.Reputation);
    this.creationDate = String(xml.CreationDate);
    this.displayName = String(xml.DisplayName);
    if(xml.LastAccessDate) this.lastAccessDate = String(xml.LastAccessDate);
    if(xml.WebsiteUrl)     this.websiteUrl = String(xml.WebsiteUrl);
    if(xml.Location)       this.location = String(xml.Location);
    if(xml.AboutMe)        this.aboutMe = String(xml.AboutMe);
    this.views = parseInt(xml.Views);
    this.upVotes = parseInt(xml.UpVotes);
    this.downVotes = parseInt(xml.DownVotes);
    if(xml.Age)            this.age = parseInt(xml.Age);
    this.accountId = parseInt(xml.AccountId);
  }
}

function readUsers(filename, generator) {
  let fileStream = fs.createReadStream(filename, {encoding: 'utf-8'});
  let saxParser  = sax.createStream(true, {trim: true, strictEntities: true});
  let streamer = new saxpath.SaXPath(saxParser, '/users/row');
	let it = generator();
  
  streamer.on('match', function(xml) {
    // Ok we have a post -- now let's be lazy and convert it :-)
    parseString(xml, function(err, result) {
      // create adequate type and pass out to yield()
      let user = new User(result.row.$);
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

exports.User = User;
exports.readUsers = readUsers;
