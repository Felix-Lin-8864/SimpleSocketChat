const express = require('express');
const { createServer } = require('node:http');
const { join } = require('node:path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { availableParallelism } = require('node:os');
const cluster = require('node:cluster');
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');

if (cluster.isPrimary) {
	const numCPUs = availableParallelism();
	for (let i = 0; i < numCPUs; i++) {
		cluster.fork({
			PORT: 3000 + i
		});
	}

	return setupPrimary();
}

async function main() {
	const db = await open({
		filename: 'chat.db',
		driver: sqlite3.Database,
	});

	await db.exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_offset TEXT UNIQUE,
			content TEXT
		);
	`);

	const app = express();
	const server = createServer(app);
	// create instance of socket.io using the HTTP server object with a 
	// cluster thread adapter
	const io = new Server(server, {
		connectionStateRecovery: {},
		adapter: createAdapter()
	});
	
	app.get('/', (req, res) => {
		res.sendFile(join(__dirname, 'index.html'));
	});
	
	// listen on the connection event for incoming sockets from the host 
	io.on('connection', async (socket) => {
		io.emit('chat message', `User ${socket.id} has connected`, -1);
		socket.on('disconnect', () => {
			io.emit('chat message', `User ${socket.id} has disconnected`, -1);
		});
	
		socket.on('chat message', async (msg, clientOffset, callback) => {
			const myMsg = `${socket.id}: ${msg}`;

			let result;
			try {
				result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', myMsg, clientOffset);
			} catch (e) {
				// sql constraint failed (i.e. pkey alr exists)
				if (e.errno === 19) {
					console.log(`pkey alr exists: ${myMsg}`);
					callback();
				} else {
					console.log(e);
				}
				return;
			}

			io.emit('chat message', myMsg, result.lastID);
			console.log('message' + myMsg);
			callback();
		});

		if (!socket.recovered) {
			try {
				await db.each('SELECT id, content FROM messages WHERE id > ?',
					[socket.handshake.auth.serverOffset || 0],
					(_err, row) => {
						socket.emit('chat message', row.content, row.id);
					}
				)
			} catch (e) {
				return;
			}
		}
	});

	const port = process.env.PORT;

	server.listen(port, () => {
		console.log(`server running at http://localhost:${port}`);
	});
}

main();