// build_index.js (R2 version)
//
// Scan R2 bucket objects under data/posts/<userId>/<postId>.json
// and build a compact index file stored back in R2 as data/posts_index.json.
//
// Run this whenever you add new harvested posts to R2:
//    node build_index.js
//
// Required environment variables:
//   R2_ENDPOINT            - e.g. https://87608ec3a41373ce7bc458d103de0201.r2.cloudflarestorage.com
//   R2_BUCKET              - e.g. viner
//   R2_ACCESS_KEY_ID
//   R2_SECRET_ACCESS_KEY
// Optional:
//   R2_DATA_PREFIX         - root prefix for archive data (default: "data")
//
// NOTE: This version does NOT use the local filesystem for posts/index;
// everything is read from and written to R2.

const { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { Readable } = require("stream");

// ---- R2 config ----

const R2_ENDPOINT = process.env.R2_ENDPOINT;
const R2_BUCKET = process.env.R2_BUCKET || "viner";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_DATA_PREFIX = (process.env.R2_DATA_PREFIX || "data").replace(/^\/+|\/+$/g, ""); // trim slashes

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

// Utility: turn R2 Body stream into a string
async function streamToString(body) {
    if (typeof body === "string") return body;
    const chunks = [];
    for await (const chunk of Readable.from(body)) {
        chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
    }
    return Buffer.concat(chunks).toString("utf8");
}

function joinKey(...parts) {
    return parts
        .filter(Boolean)
        .map((p) => String(p).replace(/^\/+|\/+$/g, ""))
        .join("/");
}

async function* listAllObjects(prefix) {
    let ContinuationToken;
    for (;;) {
        const resp = await s3.send(
            new ListObjectsV2Command({
                Bucket: R2_BUCKET,
                Prefix: prefix,
                ContinuationToken,
            })
        );
        if (resp.Contents) {
            for (const obj of resp.Contents) {
                if (obj.Key && !obj.Key.endsWith("/")) {
                    yield obj;
                }
            }
        }
        if (!resp.IsTruncated) break;
        ContinuationToken = resp.NextContinuationToken;
    }
}

async function main() {
    const postsPrefix = joinKey(R2_DATA_PREFIX, "posts") + "/";

    console.log("Building index from R2 prefix:", postsPrefix);

    const posts = [];

    // helper for created date
    function parseCreated(created) {
        if (!created) return 0;
        // handles "2016-07-31T22:08:15.000000"
        const match = created.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})/);
        let s = created;
        if (match) s = match[1] + "Z";
        const ts = Date.parse(s);
        return Number.isNaN(ts) ? 0 : ts;
    }

    for await (const obj of listAllObjects(postsPrefix)) {
        const key = obj.Key;
        if (!key.endsWith(".json")) continue;

        // Expect keys like: data/posts/<userId>/<postId>.json
        const rel = key.substring(postsPrefix.length);
        const parts = rel.split("/");
        if (parts.length !== 2) continue;
        const [userId, fileName] = parts;
        const postId = fileName.replace(/\.json$/, "");

        let body;
        try {
            const resp = await s3.send(
                new GetObjectCommand({
                    Bucket: R2_BUCKET,
                    Key: key,
                })
            );
            body = await streamToString(resp.Body);
        } catch (e) {
            console.warn("Failed to read object", key, e.message);
            continue;
        }

        let json;
        try {
            json = JSON.parse(body);
        } catch {
            continue;
        }

        const created = json.created || json.created_at || json.creationDate || "";

        const rec = {
            userId: String(json.userIdStr || json.userId || userId),
            postId: String(json.postIdStr || json.postId || postId),
            username: json.username || json.author || "",
            description:
                json.description ||
                json.descriptionPlain ||
                (json.caption && json.caption.text) ||
                "",
            thumbnailUrl: json.thumbnailUrl || null,
            created,
            createdTs: parseCreated(created),
            loops: json.loops || json.loopCount || 0,
            likes: json.likes || json.likeCount || 0,
            comments: json.comments || json.commentCount || 0,
            reposts: json.reposts || json.repostCount || 0,
        };

        posts.push(rec);
    }

    console.log("Posts scanned from R2:", posts.length);

    // newest first for feed
    posts.sort((a, b) => b.createdTs - a.createdTs);

    const indexKey = joinKey(R2_DATA_PREFIX, "posts_index.json");
    const jsonBody = JSON.stringify(posts, null, 2);

    await s3.send(
        new PutObjectCommand({
            Bucket: R2_BUCKET,
            Key: indexKey,
            Body: jsonBody,
            ContentType: "application/json; charset=utf-8",
        })
    );

    console.log("Index written to R2 object:", indexKey);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
