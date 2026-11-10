import { parentPort, workerData } from 'node:worker_threads';
import { SQLiteXNetStore } from '@xpell/xnet/server';
const { options, principal, registration_id, request_key, intent, now } = workerData;
const store = new SQLiteXNetStore({ ...options, _claim_key: Buffer.from(options._claim_key), _clock: () => now });
store.open();
parentPort.postMessage({ ready: true });
parentPort.once('message', () => {
  try {
    const result = intent ? store.accept(principal, intent) : store.claimNext(principal, registration_id, request_key);
    parentPort.postMessage({ result });
  } catch (e) { parentPort.postMessage({ error: e._code ?? e.message }); }
  finally { store.close(); }
});
