const redis = require('redis');
const {promisify} = require('util');
const uuid = require('uuid/v4');

const redisClient = redis.createClient(config.redis);

const redisCmd = (cmd) => promisify(redisClient[cmd]).bind(redisClient);

// integration transactions
const trx = async (org, job=(() => {}), rollback=(() => {})) => {
  const [
    rpush,
    lpop,
    lrange,
    psetex
  ] = [
    'rpush',
    'lpop',
    'lrange',
    'psetex'
  ].map((cmd) => redisCmd(cmd));

  const orgId = config.orgs[org];
  if (!orgId) {
    throw new Error(`invalid org: ${org}`);
  }

  const trxId = uuid();
  const orgTrxQ = `trx.${orgId}`;

  const maintainTrxId = async () => {
    await psetex(`trx.ids.${trxId}`, 1000, 'a');
  };

  await rpush(orgTrxQ, trxId);
  await maintainTrxId();
  const maintainLoop = setInterval(maintainTrxId, 200);

  const op = async () => {
    await trxCleanup(org);
    const [head] = await lrange(orgTrxQ, 0, 1);
    if (head === trxId) {
      try {
        const res = await job();
        clearInterval(maintainLoop);
        await lpop(orgTrxQ);
        return res;
      } catch (err) {
        await rollback(err);
        clearInterval(maintainLoop);
        await lpop(orgTrxQ);
        throw err;
      }
    } else {
      await new Promise((r) => setTimeout(r, 60));
      return await op();
    }
  };

  return await op();
};

const trxCleanup = async (org) => {
  const orgId = config.orgs[org];
  if (!orgId) {
    throw new Error(`invalid org: ${org}`);
  }

  const pre = config.redis.prefix;

  const cleanupLua = `
local trxIds = redis.call('lrange', '${pre}trx.${orgId}', 0, -1)
for i=1,#trxIds do
  local valid = redis.call('exists', '${pre}trx.ids.'..trxIds[i]) == 1
  if valid == false then
    redis.call('lrem', '${pre}trx.${orgId}', 0, trxIds[i])
  end
end
`
  ;

  const rEval = redisCmd('eval');
  await rEval(cleanupLua, 0);
};

