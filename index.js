// index.js (R2-backed API server)
//
// Fast server backed by a prebuilt index stored in R2 (data/posts_index.json)
// instead of scanning all JSON files on local disk.
//
// APIs:
//   GET /api/users
//   GET /api/feed?limit=100
//   GET /api/search?q=term
//   GET /api/users/:userId/posts
//   GET /api/users/:userId/posts/:postId
//   GET /api/lookup/post/:postId
//
// All post JSON files are read from R2 at:
//   data/posts/<userId>/<postId>.json
//
// Required environment variables:
//   R2_ENDPOINT
//   R2_BUCKET
//   R2_ACCESS_KEY_ID
//   R2_SECRET_ACCESS_KEY
// Optional:
//   R2_DATA_PREFIX  (default: "data")

const http = require("http");
const path = require("path");
const url = require("url");
const fs = require("fs");
const fsp = require("fs/promises");
const { Readable } = require("stream");
const {
    S3Client,
    GetObjectCommand,
} = require("@aws-sdk/client-s3");

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

// --- R2 config ---

const R2_ENDPOINT = process.env.R2_ENDPOINT;
const R2_BUCKET = process.env.R2_BUCKET || "viner";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_DATA_PREFIX = (process.env.R2_DATA_PREFIX || "data").replace(/^\/+|\/+$/g, "");

if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
    console.error("Missing required R2 env vars. Need R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY");
    process.exit(1);
}

const s3 = new S3Client({
    region: "auto",
    endpoint: R2_ENDPOINT,
    credentials: {
        accessKeyId: R2_ACCESS_KEY_ID,
        secretAccessKey: R2_SECRET_ACCESS_KEY,
    },
    forcePathStyle: true,
});

function joinKey(...parts) {
    return parts
        .filter(Boolean)
        .map((p) => String(p).replace(/^\/+|\/+$/g, ""))
        .join("/");
}

async function streamToString(body) {
    if (typeof body === "string") return body;
    const chunks = [];
    for await (const chunk of Readable.from(body)) {
        chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
    }
    return Buffer.concat(chunks).toString("utf8");
}

// --- Local static/public paths ---

const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");

// ---- In-memory index structures ----

/**
 * usersIndex: {
 *   [userId]: {
 *     userId,
 *     username,
 *     postCount,
 *     posts: [PostMeta, ...]   // sorted oldest → newest per user
 *   }
 * }
 *
 * allPosts: [PostMeta, ...]    // global, newest → oldest
 *
 * postIdIndex: Map<postId, PostMeta>
 */

let usersIndex = {};
let allPosts = [];
const postIdIndex = new Map();

// ------------ Helpers ------------

function sendJSON(res, status, data) {
    const body = JSON.stringify(data);
    res.writeHead(status, {
        "Content-Type": "application/json; charset=utf-8",
        "Content-Length": Buffer.byteLength(body),
    });
    res.end(body);
}

function sendText(res, status, text) {
    res.writeHead(status, {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Length": Buffer.byteLength(text),
    });
    res.end(text);
}

async function serveStatic(req, res, pathname) {
    let filePath = path.join(PUBLIC_DIR, pathname.replace(/^\/+/, ""));
    if (pathname === "/" || pathname === "") {
        filePath = path.join(PUBLIC_DIR, "index.html");
    }

    try {
        const stat = await fsp.stat(filePath);
        if (!stat.isFile()) {
            return false;
        }
        const stream = fs.createReadStream(filePath);
        res.writeHead(200, { "Content-Type": guessContentType(filePath) });
        stream.pipe(res);
        return true;
    } catch {
        return false;
    }
}

function guessContentType(p) {
    const ext = path.extname(p).toLowerCase();
    if (ext === ".html" || ext === ".htm") return "text/html; charset=utf-8";
    if (ext === ".js") return "text/javascript; charset=utf-8";
    if (ext === ".css") return "text/css; charset=utf-8";
    if (ext === ".json") return "application/json; charset=utf-8";
    if (ext === ".png") return "image/png";
    if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
    if (ext === ".gif") return "image/gif";
    return "application/octet-stream";
}

// ------------ Load index from R2 ------------

async function loadIndex() {
    const indexKey = joinKey(R2_DATA_PREFIX, "posts_index.json");
    console.log("Loading index from R2 object:", indexKey);

    let body;
    try {
        const resp = await s3.send(
            new GetObjectCommand({
                Bucket: R2_BUCKET,
                Key: indexKey,
            })
        );
        body = await streamToString(resp.Body);
    } catch (e) {
        throw new Error(
            `Index file not found in R2 at ${indexKey}\nMake sure build_index.js has been run. (${e.message})`
        );
    }

    /** @type {Array} */
    let posts;
    try {
        posts = JSON.parse(body);
    } catch (e) {
        throw new Error("Failed to parse posts_index.json from R2: " + e.message);
    }

    allPosts = [];
    usersIndex = {};
    postIdIndex.clear();

    for (const p of posts) {
        const userId = String(p.userId);
        const postId = String(p.postId);
        const username = p.username || "";

        // Fill usersIndex
        let u = usersIndex[userId];
        if (!u) {
            u = {
                userId,
                username,
                postCount: 0,
                posts: [],
            };
            usersIndex[userId] = u;
        }
        if (!u.username && username) {
            u.username = username;
        }
        u.posts.push(p);

        postIdIndex.set(postId, p);
        allPosts.push(p);
    }

    // Sort posts per user oldest → newest
    for (const u of Object.values(usersIndex)) {
        u.posts.sort((a, b) => a.createdTs - b.createdTs);
        u.postCount = u.posts.length;
    }

    // allPosts is already newest → oldest from build_index.js,
    // but we can ensure:
    allPosts.sort((a, b) => b.createdTs - a.createdTs);

    console.log(
        `Loaded index from R2: ${Object.keys(usersIndex).length} users, ${allPosts.length} posts`
    );
}

// ------------ Fetch single post JSON from R2 ------------

async function fetchPostJson(userId, postId) {
    const numericPostId = postId.replace(/[^0-9]/g, "");
    const key = joinKey(R2_DATA_PREFIX, "posts", userId, numericPostId + ".json");

    try {
        const resp = await s3.send(
            new GetObjectCommand({
                Bucket: R2_BUCKET,
                Key: key,
            })
        );
        const body = await streamToString(resp.Body);
        return JSON.parse(body);
    } catch (e) {
        console.error("Error reading post JSON from R2:", key, e.message);
        return null;
    }
}

// ------------ HTTP server ------------

const server = http.createServer(async (req, res) => {
    const parsed = url.parse(req.url || "", true);
    const pathname = parsed.pathname || "/";

    // Basic healthcheck
    if (pathname === "/healthz") {
        return sendText(res, 200, "ok");
    }

    // Try static assets first (public/)
    if (await serveStatic(req, res, pathname)) {
        return;
    }

    // API routes
    if (pathname.startsWith("/api/")) {
        // /api/users
        if (pathname === "/api/users" && req.method === "GET") {
            const users = Object.values(usersIndex).map((u) => ({
                userId: u.userId,
                username: u.username,
                postCount: u.postCount,
            }));
            return sendJSON(res, 200, users);
        }

        // /api/feed?limit=100
        if (pathname === "/api/feed" && req.method === "GET") {
            let limit = Number(parsed.query.limit || 100);
            if (!Number.isFinite(limit) || limit <= 0) limit = 100;
            if (limit > 500) limit = 500;
            return sendJSON(res, 200, allPosts.slice(0, limit));
        }

        // /api/search?q=term
        if (pathname === "/api/search" && req.method === "GET") {
            const q = String(parsed.query.q || "").trim().toLowerCase();
            if (!q) return sendJSON(res, 200, []);

            const results = allPosts.filter((p) => {
                const hay =
                    (p.description || "") +
                    " " +
                    (p.username || "") +
                    " " +
                    (p.userId || "");
                return hay.toLowerCase().includes(q);
            });

            return sendJSON(res, 200, results.slice(0, 200));
        }

        // /api/users/:userId/posts
        const userPostsMatch = pathname.match(/^\/api\/users\/([^/]+)\/posts$/);
        if (userPostsMatch && req.method === "GET") {
            const userId = decodeURIComponent(userPostsMatch[1]);
            const user = usersIndex[userId];
            if (!user) return sendText(res, 404, "User not found");
            return sendJSON(res, 200, user.posts);
        }

        // /api/users/:userId/posts/:postId  (full JSON from R2)
        const postMatch = pathname.match(/^\/api\/users\/([^/]+)\/posts\/([^/]+)$/);
        if (postMatch && req.method === "GET") {
            const userId = decodeURIComponent(postMatch[1]);
            const postId = decodeURIComponent(postMatch[2]);

            const json = await fetchPostJson(userId, postId);
            if (!json) return sendText(res, 404, "Post not found");
            return sendJSON(res, 200, json);
        }

        // /api/lookup/post/:postId  (full JSON from R2, lookup by index first)
        const lookupMatch = pathname.match(/^\/api\/lookup\/post\/([^/]+)$/);
        if (lookupMatch && req.method === "GET") {
            const postId = decodeURIComponent(lookupMatch[1]);
            const rec = postIdIndex.get(String(postId));
            if (!rec) return sendText(res, 404, "Post not found in index");
            const userId = String(rec.userId);

            const json = await fetchPostJson(userId, postId);
            if (!json) return sendText(res, 404, "Post not found");
            return sendJSON(res, 200, json);
        }

        // unknown API
        return sendText(res, 404, "Not found");
    }

    // Fallback: serve index.html for any non-API route (SPA style)
    try {
        const indexHtml = await fsp.readFile(path.join(PUBLIC_DIR, "index.html"), "utf8");
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(indexHtml);
    } catch {
        return sendText(res, 404, "Not found");
    }
});

// ------------ Startup ------------

loadIndex()
    .then(() => {
        server.listen(PORT, () => {
            console.log(`Server running at http://localhost:${PORT}`);
        });
    })
    .catch((err) => {
        console.error(err.message);
        process.exit(1);
    });
