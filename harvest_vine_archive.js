// harvest_vine_archive.js
// Collect media URLs via archive.vine.co/profiles + posts,
// rewrite v.cdn.vine.co -> https://vines.s3.amazonaws.com,
// and save unique URLs into media_urls.json

const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");

// ---- CONFIG ----

// Text file listing profile identifiers, one per line.
// Each line can be:
//   - numeric userId:  "942914934646415360"
//   - profile URL:     "https://vine.co/u/942914934646415360"
//   - vanity URL:      "https://vine.co/itsruthb"
//   - vanity name:     "itsruthb"  (we'll treat as vanity)
const PROFILES_LIST_FILE = "profiles.txt";

const OUTPUT_DIR = "vine_archive_harvest";
const OUTPUT_URLS_FILE = path.join(OUTPUT_DIR, "media_urls.json");

// Base endpoints
const ARCHIVE_PROFILE_URL = (id) => `https://archive.vine.co/profiles/${id}.json`;
const ARCHIVE_POST_URL = (id) => `https://archive.vine.co/posts/${id}.json`;

// For vanity → userId
// (this is how youtube-dl did it: api/users/profiles/vanity/{user} ) :contentReference[oaicite:2]{index=2}
const VINE_PROFILE_VANITY_API = (vanity) =>
    `https://vine.co/api/users/profiles/vanity/${encodeURIComponent(vanity)}`;

// Rewrite media host
const MEDIA_TARGET_BASE = "https://vines.s3.amazonaws.com";

// politeness
const REQUEST_DELAY_MS = 200;

// ---- FETCH POLYFILL (for older Node) ----

if (typeof fetch === "undefined") {
    global.fetch = (...args) =>
        import("node-fetch").then(({ default: f }) => f(...args));
}

// ---- UTIL ----

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJSON(url, label = "") {
    console.log(`[GET JSON] ${label || url}`);
    try {
        const res = await fetch(url, {
            headers: {
                "User-Agent": "VineArchiveHarvester/1.0",
                Accept: "application/json,text/json;q=0.9,*/*;q=0.8",
            },
            timeout: 15000,
        }).catch((e) => {
            throw e;
        });

        if (!res.ok) {
            console.log(`  ! HTTP ${res.status} for ${url}`);
            return null;
        }
        const data = await res.json();
        await sleep(REQUEST_DELAY_MS);
        return data;
    } catch (e) {
        console.log(`  ! Error fetching ${url}:`, e.message || e);
        return null;
    }
}

function parseProfileLine(lineRaw) {
    const line = lineRaw.trim();
    if (!line || line.startsWith("#")) return null;

    // numeric user id
    if (/^[0-9]+$/.test(line)) {
        return { type: "userId", value: line };
    }

    // URL
    if (line.startsWith("http://") || line.startsWith("https://")) {
        try {
            const u = new URL(line);
            const host = u.hostname.toLowerCase();
            if (!host.includes("vine.co")) {
                console.log(`  ! Skipping non-vine URL: ${line}`);
                return null;
            }
            const parts = u.pathname.split("/").filter(Boolean); // ["u","123"] or ["itsruthb"]

            if (parts.length >= 2 && parts[0] === "u" && /^[0-9]+$/.test(parts[1])) {
                return { type: "userId", value: parts[1] };
            }

            // treat first path segment as vanity
            if (parts.length >= 1) {
                return { type: "vanity", value: parts[0] };
            }
        } catch {
            // fall through to vanity guess
        }
    }

    // fallback: treat as vanity
    return { type: "vanity", value: line };
}

async function resolveUserId(entry) {
    if (!entry) return null;

    if (entry.type === "userId") {
        return entry.value;
    }

    if (entry.type === "vanity") {
        const vanity = entry.value;
        const url = VINE_PROFILE_VANITY_API(vanity);
        const data = await fetchJSON(url, `vanity ${vanity}`);
        if (!data || !data.data) {
            console.log(`  ! Could not resolve vanity "${vanity}"`);
            return null;
        }
        const d = data.data;
        const userId = d.userId || d.userIdStr;
        if (!userId) {
            console.log(`  ! No userId in vanity response for "${vanity}"`);
            return null;
        }
        console.log(`  → vanity "${vanity}" → userId ${userId}`);
        return String(userId);
    }

    return null;
}

function extractMediaUrlsFromPost(postJson) {
    const urls = [];

    if (!postJson || typeof postJson !== "object") return urls;

    // Strategy: grab any string field that looks like a URL
    // and contains "http".
    const stack = [postJson];

    while (stack.length) {
        const obj = stack.pop();
        for (const [key, value] of Object.entries(obj)) {
            if (value && typeof value === "object") {
                stack.push(value);
            } else if (typeof value === "string" && value.startsWith("http")) {
                urls.push(value);
            }
        }
    }

    return urls;
}

function rewriteMediaUrl(url) {
    try {
        const u = new URL(url);
        const host = u.hostname.toLowerCase();

        if (
            host.includes("v.cdn.vine.co") ||
            host.includes("amazonaws.com") ||
            host.includes("vines.s3.amazonaws.com")
        ) {
            const cleanPath = u.pathname.replace(/^\/+/, ""); // no leading slashes
            const qs = u.search || "";
            return `${MEDIA_TARGET_BASE}/${cleanPath}${qs}`;
        }
        // Non-media or other hosts: return unchanged
        return url;
    } catch {
        return url;
    }
}

// ---- MAIN LOGIC ----

async function readProfileEntries() {
    const txt = await fsp.readFile(PROFILES_LIST_FILE, "utf8");
    const lines = txt.split(/\r?\n/);
    const entries = [];

    for (const line of lines) {
        const parsed = parseProfileLine(line);
        if (parsed) entries.push(parsed);
    }

    return entries;
}

async function processProfile(userId, allUrlsSet) {
    console.log(`\n=== Profile ${userId} ===`);
    const url = ARCHIVE_PROFILE_URL(userId);
    const profile = await fetchJSON(url, `archive profile ${userId}`);
    if (!profile) {
        console.log(`  ! No profile JSON for ${userId}`);
        return;
    }

    const posts = Array.isArray(profile.posts) ? profile.posts : [];
    console.log(`  found ${posts.length} posts`);

    for (let i = 0; i < posts.length; i++) {
        const postId = posts[i];
        if (!postId) continue;
        console.log(`  [${i + 1}/${posts.length}] post ${postId}`);
        const postUrl = ARCHIVE_POST_URL(postId);
        const postJson = await fetchJSON(postUrl, `archive post ${postId}`);
        if (!postJson) continue;

        const rawUrls = extractMediaUrlsFromPost(postJson);
        for (const raw of rawUrls) {
            const rewritten = rewriteMediaUrl(raw);
            allUrlsSet.add(rewritten);
        }
    }
}

async function main() {
    await fsp.mkdir(OUTPUT_DIR, { recursive: true });

    let entries;
    try {
        entries = await readProfileEntries();
    } catch (e) {
        console.error(
            `Could not read ${PROFILES_LIST_FILE}. Create it with one profile per line.`
        );
        console.error("Example lines:");
        console.error("  942914934646415360");
        console.error("  https://vine.co/u/942914934646415360");
        console.error("  https://vine.co/itsruthb");
        console.error("  itsruthb");
        process.exit(1);
    }

    console.log(`Loaded ${entries.length} profile entries from ${PROFILES_LIST_FILE}`);

    const allUrls = new Set();

    for (let i = 0; i < entries.length; i++) {
        console.log(`\n--- [${i + 1}/${entries.length}] ---`);
        const entry = entries[i];
        const userId = await resolveUserId(entry);
        if (!userId) continue;
        await processProfile(userId, allUrls);
    }

    const arrayUrls = Array.from(allUrls).sort();

    await fsp.writeFile(
        OUTPUT_URLS_FILE,
        JSON.stringify(arrayUrls, null, 2),
        "utf8"
    );

    console.log(`\nDone. Saved ${arrayUrls.length} unique URLs to ${OUTPUT_URLS_FILE}`);
}

main().catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
});
