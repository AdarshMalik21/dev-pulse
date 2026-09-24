import express from "express";
import cors from "cors";
import { MongoClient } from "mongodb";
import { createClient } from "redis";

const app = express();
app.use(cors());
app.use(express.json());

const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.on("error", (e) => console.log("Redis Client Error", e));

async function redisConnect() {
  await redisClient.connect();
  console.log("Connected to Redis");
}

const mongoURI = process.env.MONGODB_URI;
if (!mongoURI || !process.env.REDIS_URL) {
  throw new Error(
    "MONGODB_URI and REDIS_URL environment variables are required",
  );
}
const client = new MongoClient(mongoURI);

let eventsCollection;

async function connect() {
  await client.connect();
  const db = client.db("dev-pulse");
  eventsCollection = db.collection("events");

  await eventsCollection.createIndex({ timestamp: -1 });
  console.log("Index created on timestamp field");

  await eventsCollection.createIndex({ endpoint: 1, timestamp: -1 });
  console.log("Compount index created");
  console.log("Connected to MongoDB");
}

const events = [];

app.post("/ingest", async (req, res) => {
  const event = req.body;
  event.timestamp = new Date(event.timestamp);
  await eventsCollection.insertOne(event);
  res.status(201).json({ status: "oky" });
});

app.get("/events", async (req, res) => {
  const recent = await eventsCollection
    .find({})
    .sort({ timestamp: -1 })
    .limit(50)
    .toArray();
  res.json(recent);
});

app.get("/debug/explain", async (req, res) => {
  const explanation = await eventsCollection
    .find({})
    .sort({ timestamp: -1 })
    .limit(50)
    .explain("executionStats");

  res.json({
    executionTimeMillis: explanation.executionStats.executionTimeMillis,
    totalDocsExamined: explanation.executionStats.totalDocsExamined,
    totalKeysExamined: explanation.executionStats.totalKeysExamined,
    nReturned: explanation.executionStats.nReturned,
  });
});

app.get("/debug/explain-by-endpoint", async (req, res) => {
  const endpoint = req.query.endpoint || "/checkout";

  const explanation = await eventsCollection
    .find({ endpoint })
    .sort({ timestamp: -1 })
    .limit(50)
    .explain("executionStats");

  res.json({
    endpoint,
    executionTimeMillis: explanation.executionStats.executionTimeMillis,
    totalDocsExamined: explanation.executionStats.totalDocsExamined,
    totalKeysExamined: explanation.executionStats.totalKeysExamined,
    nReturned: explanation.executionStats.nReturned,
  });
});

app.get("/metrics", async (req, res) => {
  const cachedKey = "metrics:v2";

  const cached = await redisClient.get(cachedKey);
  if (cached) {
    console.log("cache hit");
    return res.json(JSON.parse(cached));
  }

  const metrics = await eventsCollection
    .aggregate([
      {
        $match: { timestamp: { $gte: new Date(Date.now() - 60 * 60 * 1000) } },
      },
      {
        $group: {
          _id: "$endpoint",
          totalRequests: { $sum: 1 },
          errorCount: { $sum: { $cond: [{ $gte: ["$status", 400] }, 1, 0] } },
          avgResponseTime: { $avg: "$responseTime" },
        },
      },

      { $sort: { totalRequests: -1 } },
    ])
    .toArray();

  await redisClient.set(cachedKey, JSON.stringify(metrics), { EX: 60 });
  console.log("cache miss");
  res.json(metrics);
});

app.get("/metrics/timeseries", async (req, res) => {
  const raw = await eventsCollection
    .aggregate([
      {
        $match: {
          timestamp: { $gte: new Date(Date.now() - 60 * 60 * 1000) },
        },
      },
      {
        $group: {
          _id: {
            endpoint: "$endpoint",
            minute: { $dateTrunc: { date: "$timestamp", unit: "minute" } },
          },
          count: { $sum: 1 },
        },
      },
      {
        $sort: { "_id.minute": 1 },
      },
    ])
    .toArray();

  const grouped = {};
  raw.forEach((row) => {
    const timeLabel = row._id.minute.toISOString().slice(11, 16); // "HH:MM"
    if (!grouped[timeLabel]) grouped[timeLabel] = { time: timeLabel };
    grouped[timeLabel][row._id.endpoint] = row.count;
  });

  res.json(Object.values(grouped));
});

app.listen(4000, async () => {
  await connect();
  await redisConnect();
  console.log("Server is running on port 4000");
});
