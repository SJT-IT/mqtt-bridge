const admin = require("firebase-admin");
const mqtt = require("mqtt");
require("dotenv").config();

// ================= FIREBASE INIT =================
const serviceAccount = require("./serviceAccount.json");
// const serviceAccount = JSON.parse(
//   process.env.FIREBASE_SERVICE_ACCOUNT
// );

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DB_URL
});

const db = admin.database();

// ================= MQTT INIT =================
const client = mqtt.connect(process.env.MQTT_BROKER_URL);

// ================= CONNECT =================
client.on("connect", () => {

  console.log("✅ MQTT connected to HiveMQ");

  // Listen to ALL device ACKs
  client.subscribe("devices/+/ack", (err) => {

    if (err) {
      console.log("❌ ACK subscribe failed");
    } else {
      console.log("✅ Listening for ACKs (all devices)");
    }
  });
});

// ================= ERROR =================
client.on("error", (err) => {
  console.error("❌ MQTT error:", err);
});

// =====================================================
// ================= FIREBASE WATCHER ==================
// =====================================================
const devicesRef = db.ref("devices");

devicesRef.on("child_added", (deviceSnapshot) => {

  const deviceId = deviceSnapshot.key;

  console.log(`📡 Watching device: ${deviceId}`);

  const commandsRef = db.ref(`devices/${deviceId}/commands`);

  commandsRef.on("child_added", (snapshot) => {

    const command = snapshot.val();
    const commandId = snapshot.key;

    console.log(`\n📥 Command for ${deviceId}/${commandId}`);
    console.log(command);

    // Prevent duplicate processing
    if (command.status === "sent" || command.status === "received") {
      return;
    }

    // ================= MQTT PAYLOAD =================
    const payload = JSON.stringify({
      deviceId: deviceId,
      commandId: commandId,
      type: command.type,
      value: command.value,
      SOC: command.SOC || 100
    });

    console.log("📡 Publishing:", payload);

    // ================= MQTT PUBLISH =================
    client.publish(
      `devices/${deviceId}/commands`,
      payload,
      { qos: 1 },

      async (err) => {

        if (err) {
          console.error("❌ Publish failed:", err);
          return;
        }

        // ================= UPDATE FIREBASE =================
        await db.ref(`devices/${deviceId}/commands/${commandId}`)
        .update({
          status: "sent",
          senttimestamp: Date.now(),

          // only initialize once
          receivedtimestamp:
            command.receivedtimestamp || 0
        });

        console.log(` Sent → ${deviceId}/${commandId}`);
      }
    );
  });
});

// =====================================================
// ================= MQTT ACK HANDLER ==================
// =====================================================
client.on("message", async (topic, message) => {

  try {

    const ack = JSON.parse(message.toString());

    console.log("\n📩 ACK RECEIVED:");
    console.log(ack);

    const { deviceId, commandId } = ack;

    if (!deviceId || !commandId) {
      console.log("❌ Invalid ACK");
      return;
    }

    await db.ref(`devices/${deviceId}/commands/${commandId}`)
      .update({
        status: "received",
        receivedtimestamp: Date.now()
      });

    console.log(`✅ Updated Firebase → ${deviceId}/${commandId}`);

  } catch (err) {

    console.error("❌ ACK processing error:", err);
  }
});

console.log("🚀 Bridge Running (Multi-Device Ready)");