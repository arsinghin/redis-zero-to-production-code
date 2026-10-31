// Part 1: the same first commands from Node.js (node-redis).
const { createClient } = require('redis');

const client = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
client.on('error', (err) => console.error('Redis error:', err.message));

async function main() {
  await client.connect();

  await client.set('pizza:6', 'Meat Lovers - $16');
  console.log(await client.get('pizza:6'));   // Meat Lovers - $16

  await client.set('orders:node', 0);
  await client.incr('orders:node');
  console.log(await client.get('orders:node')); // 1

  await client.del(['pizza:6', 'orders:node']);
  await client.close();                       // waits for pending replies, then disconnects
}

main();
