// Initialize node.js modules as variables
var express = require('express'),
	app = express(),
	bodyParser = require('body-parser'),
	morgan = require('morgan'),
	users = require('./lib/users');

module.exports = app;

app.use(morgan('dev')); // will log http requests on console
app.use(bodyParser.json()); // will automatically parse the body in HTTP post requests, available in req.body

// route to GET /users, returns all users
app.get('/users', function(req, res) { 
	users.getUsers(function(err, result) { // calls getUsers() from ./lib/users, takes callback function as single argument
		if (err) {
			return res.status(500).json({ success: false, reason: err.message });
		}
		res.send({ success: true, users: result}); // return all users in plain json
	});
});

// route to GET /users/[id], returns an individual user, i.e. /users/1 returns user 1
app.get('/users/:id', function(req, res) { // ':id' takes whatever is following after '/users/'
	var id = req.params.id;                // and makes that available in req.params.id.

	users.getUser(id, function(err, result) { // calls getUser() from ./lib/users, takes callback function as single argument
		if (err) { // if error, return status 400
			return res.status(400).json({ success: false, reason: err.message });
		}
		if (!result) { // if no result (no user), return 404
			return res.status(404).json({ success: false, reason: 'user id1 unknown'});
		}

		res.send({ success: true, user: result}); // return single user in plain json
	});
});


/*

//by marcelo saied at app.js
app.get('/users/name/:name', function(req, res) { // ':id' takes whatever is following after '/users/name'
	var name = req.params.name;                // and makes that available in req.params.id.

	users.getUserByName(name, function(err, result) { // calls getUserByName() from ./lib/users, takes callback function as single argument
        
		if (err) { // if error, return status 400
			return res.status(400).json({ success: false, reason: err.message });
		}
		if (!result) { // if no result (no user), return 404
			return res.status(404).json({ success: false, reason: 'user name unknown'});
		}

			res.send({ success: true, user: result}); // return single user in plain json

		
	});
});






// route to POST /users, for creating new users
app.post('/users', function(req, res) {
	var user = req.body; // bodyParser will make the body of the HTTP POST request available in req.body

	users.addUser(user, function(err, result) { // calls addUser(), takes user from POST request and callback as arguments
		if (err) { // if error, return 400
			return res.status(400).json({ success: false, reason: err.message });
		}

		res.send({ success: true, user: result}); // return just created user in plain json
	});
});

