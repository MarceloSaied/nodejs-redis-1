// Initiliaze empty 'database', just a simple array for storing users 
var users = [];

// getUsers returns all users
exports.getUsers = function(callback) {
	process.nextTick(function() { // Does not delay execution.
		callback(null, users);
	});
};

// getUser returns a single user based on 'id' field
exports.getUser = function(id, callback) {
	process.nextTick(function() {
		var i, user;

		// Scan array of users for user with requested id
		for (i = 0; i < users.length; i++) {
			user = users[i];
			if (user.id === id) return callback(null, user); // if the requested user is found, return with this user
		}

		// not found, returns with no user
		callback();			
	});
};

// addUser adds a user to the users array
// Any arbitrary field in the user object will be kept, an eventual id field will be overwritten
exports.addUser = function(user, callback) {
	process.nextTick(function() {
		if (!user.name) return new Error('missing user name'); // the user object in the POST request will require a 'name' field
		var id = (users.length + 1).toString(); // id is the length of the array incremented by 1
		user.id = id;
		users.push(user); // add user to users array
		callback(null, user); // return with the just added user
	});
};


//added by marcelo saied  at users.js
// getUser returns a single user based on 'id' field
exports.getUserByName = function(name, callback) {
	process.nextTick(function() {
		var i, user;

		// Scan array of users for user with requested id
		for (i = 0; i < users.length; i++) {
			user = users[i];
			if (user.name == name) return callback(null, user); // if the requested user is found, return with this user
		}

		// not found, returns with no user
		callback();			
	});
};