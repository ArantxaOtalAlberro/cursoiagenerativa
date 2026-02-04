"use strict";

// Safer parsing: avoid `eval` and never log sensitive fields like passwords.
function parseUserInput(input) {
	try {
		// Expect JSON from the client; parsing is safe compared to eval
		return JSON.parse(input);
	} catch (err) {
		console.error('Invalid input format');
		return null;
	}
}

function handleLogin(user) {
	if (!user || typeof user.username !== 'string') return false;
	// Do NOT log user.password or any sensitive field.
	// Replace this stub with a real authentication call.
	const username = user.username;
	const password = user.password; // keep local but never log it
	// Example check (placeholder only)
	return username === 'admin' && password === 'secret';
}

// Example usage without exposing sensitive information
const exampleInput = '{"username":"admin","password":"secret"}';
const user = parseUserInput(exampleInput);
if (user) {
	const ok = handleLogin(user);
	console.log('Login success:', ok);
}

