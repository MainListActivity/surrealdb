// Local disposable-server acceptance. Never accepts a remote host.
import assert from "node:assert/strict";

const endpoint = new URL(process.argv[2]);
assert.equal(endpoint.hostname, "127.0.0.1");
assert.equal(endpoint.port, "18000");
assert.ok(["http:", "ws:"].includes(endpoint.protocol));
const sdk = await import(process.env.NATIVE_QUOTA_JS_SDK_MODULE || "surrealdb");
const db = new sdk.Surreal();
const timer = setTimeout(() => { console.error("SDK acceptance timed out"); process.exit(1); }, 45000);
try {
  if (process.argv.includes("--expect-version-rejection")) {
    await assert.rejects(db.connect(endpoint), error => error.name === "UnsupportedVersionError");
    console.log("PASS: incompatible SDK refuses server version");
  } else {
    await db.connect(endpoint);
    await db.signin({ username: "root", password: "root" });
    const ns = `disposable_sdk_acceptance_${Date.now()}`;
    const database = endpoint.protocol === "ws:" ? "websocket" : "http";
    await db.query(`DEFINE NAMESPACE IF NOT EXISTS ${ns}`);
    await db.use({ namespace: ns });
    await db.query(`DEFINE DATABASE IF NOT EXISTS ${database}`);
    await db.use({ namespace: ns, database });
    await db.query("DEFINE QUOTA ON DATABASE " + database +
      " RULE acceptance_records FOR RECORD MATCH EXACT message LIMIT 2");
    await db.query("CREATE message:one SET value = 1; CREATE message:two SET value = 2");
    await assert.rejects(db.query("CREATE message:three"), error =>
      error.message.includes("acceptance_records"));
    const [rows] = await db.query("SELECT * FROM message");
    assert.equal(rows.length, 2, "rejected write leaves no third row");
    if (endpoint.protocol === "ws:") {
      const subscription = await db.live(new sdk.Table("message"));
      try {
        await subscription.ready();
        let resolve;
        const notification = new Promise(done => { resolve = done; });
        const unsubscribe = subscription.subscribe(message => resolve(message));
        await db.query("UPDATE message:one SET value = 3");
        const message = await notification;
        assert.equal(message.action, "UPDATE");
        assert.equal((message.value || message.result).value, 3);
        unsubscribe();
      } finally {
        await subscription.kill();
      }
    }
    console.log(`PASS: ${endpoint.protocol} authenticated query, atomic quota rejection` +
      (endpoint.protocol === "ws:" ? ", LIVE notification" : ""));
  }
} finally {
  clearTimeout(timer);
  await db.close();
}
