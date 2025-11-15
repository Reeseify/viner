// server.js
//
// Fast server backed by a prebuilt index (data/posts_index.json)
// instead of scanning all JSON files on startup.
//
// APIs:
//   GET /api/users
//   GET /api/feed?limit=100
//   GET /api/search?q=term
//   GET /api/users/:userId/posts
//   GET /api/users/:userId/posts/:postId
//   GET /api/lookup/post/:postId
//
// All post JSON files are still served from vine_archive_harvest/posts/<userId>/<postId>.json

const http = require("http");
const fsp = require("fs/promises");
const fs = require("fs");
const path = require("path");
const url = require("url");

const PORT = 3000;

const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");
const DATA_DIR = path.join(ROOT, "data");
const POSTS_ROOT = path.join(ROOT, "vine_archive_harvest", "posts");
const INDEX_FILE = path.join(DATA_DIR, "posts_index.json");

/**
 * In-memory structures built from posts_index.json
 *
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

const usersIndex = Object.create(null);
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
    });
    res.end(text);
}

async function sendIndexHtml(res) {
    try {
        const indexPath = path.join(PUBLIC_DIR, "index.html");
        const data = await fsp.readFile(indexPath);
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(data);
    } catch (e) {
        sendText(res, 500, "index.html not found");
    }
}

async function serveStatic(req, res, pathname) {
    let filePath = path.join(PUBLIC_DIR, pathname);
    try {
        const stat = await fsp.stat(filePath);
        if (stat.isDirectory()) {
            filePath = path.join(filePath, "index.html");
        }
        const ext = path.extname(filePath).toLowerCase();
        let type = "text/plain; charset=utf-8";
        if (ext === ".html") type = "text/html; charset=utf-8";
        else if (ext === ".js") type = "text/javascript; charset=utf-8";
        else if (ext === ".css") type = "text/css; charset=utf-8";

        const data = await fsp.readFile(filePath);
        res.writeHead(200, { "Content-Type": type });
        res.end(data);
    } catch {
        return sendIndexHtml(res);
    }
}

// ------------ Index loader (FAST) ------------

async function loadIndex() {
    console.log("Loading index:", INDEX_FILE);
    const exists = fs.existsSync(INDEX_FILE);
    if (!exists) {
        throw new Error(
            `Index file not found: ${INDEX_FILE}\nRun: node build_index.js`
        );
    }

    const raw = await fsp.readFile(INDEX_FILE, "utf8");
    /** @type {Array} */
    const posts = JSON.parse(raw);
    allPosts = [];
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

        // global structures
        allPosts.push(p);
        postIdIndex.set(postId, p);
    }

    // fix postCount and per-user sorting (oldest → newest)
    for (const userId of Object.keys(usersIndex)) {
        const u = usersIndex[userId];
        u.posts.sort((a, b) => (a.createdTs || 0) - (b.createdTs || 0));
        u.postCount = u.posts.length;
    }

    // allPosts already sorted newest→oldest when build_index.js wrote it,
    // but if you want to be safe:
    allPosts.sort((a, b) => (b.createdTs || 0) - (a.createdTs || 0));

    console.log(
        "Loaded index:",
        Object.keys(usersIndex).length,
        "users,",
        allPosts.length,
        "posts"
    );
}

// ------------ API handlers ------------

async function handleApi(req, res, pathname, query) {
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
        let limit = 100;
        if (query && typeof query.limit !== "undefined") {
            const n = parseInt(query.limit, 10);
            if (!isNaN(n) && n > 0) {
                limit = Math.min(n, 500);
            }
        }
        return sendJSON(res, 200, allPosts.slice(0, limit));
    }

    // /api/search?q=term  (search title + username)
    if (pathname === "/api/search" && req.method === "GET") {
        const q = (query && query.q ? String(query.q) : "").trim();
        if (!q) {
            return sendJSON(res, 400, { error: "Missing q" });
        }
        const needle = q.toLowerCase();
        const results = [];
        for (const p of allPosts) {
            if (results.length >= 200) break;
            const desc = (p.description || "").toLowerCase();
            const user = (p.username || "").toLowerCase();
            if (desc.includes(needle) || user.includes(needle)) {
                results.push(p);
            }
        }
        return sendJSON(res, 200, results);
    }

    // /api/users/:userId/posts
    const userPostsMatch = pathname.match(/^\/api\/users\/([^/]+)\/posts$/);
    if (userPostsMatch && req.method === "GET") {
        const userId = decodeURIComponent(userPostsMatch[1]);
        const user = usersIndex[userId];
        if (!user) return sendText(res, 404, "User not found");
        return sendJSON(res, 200, user.posts);
    }

    // /api/users/:userId/posts/:postId  (full JSON from disk)
    const postMatch = pathname.match(/^\/api\/users\/([^/]+)\/posts\/([^/]+)$/);
    if (postMatch && req.method === "GET") {
        const userId = decodeURIComponent(postMatch[1]);
        const postId = decodeURIComponent(postMatch[2]);
        const postPath = path.join(
            POSTS_ROOT,
            userId,
            postId.replace(/[^0-9]/g, "") + ".json"
        );

        try {
            const raw = await fsp.readFile(postPath, "utf8");
            const json = JSON.parse(raw);
            return sendJSON(res, 200, json);
        } catch (e) {
            console.error("Error reading post JSON:", e.message);
            return sendText(res, 404, "Post not found");
        }
    }

    // /api/watch/:postId  -> full JSON for a vine by ID (no userId needed)
    const watchMatch = pathname.match(/^\/api\/watch\/([^/]+)$/);
    if (watchMatch && req.method === "GET") {
        const postId = decodeURIComponent(watchMatch[1]);
        const rec = postIdIndex.get(postId);
        if (!rec) {
            return sendText(res, 404, "Post not found");
        }

        const userId = String(rec.userId);
        const postPath = path.join(
            POSTS_ROOT,
            userId,
            postId.replace(/[^0-9]/g, "") + ".json"
        );

        try {
            const raw = await fsp.readFile(postPath, "utf8");
            const json = JSON.parse(raw);
            return sendJSON(res, 200, json);
        } catch (e) {
            console.error("Error reading watch JSON:", e.message);
            return sendText(res, 404, "Post not found");
        }
    }


    return sendText(res, 404, "Not found");
}

// ------------ HTTP server ------------

const server = http.createServer(async (req, res) => {
    try {
        const parsed = url.parse(req.url, true);
        const pathname = parsed.pathname || "/";

        if (pathname.startsWith("/api/")) {
            return handleApi(req, res, pathname, parsed.query);
        }

        return serveStatic(req, res, pathname);
    } catch (e) {
        console.error("Server error:", e);
        sendText(res, 500, "Internal server error");
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
