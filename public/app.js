// --- basic helpers ---

async function fetchJSON(path) {
    const res = await fetch(path);
    if (!res.ok) {
        throw new Error(`HTTP ${res.status} for ${path}`);
    }
    return await res.json();
}

function formatLoops(n) {
    if (n == null) return "0 loops";
    const num = Number(n) || 0;
    const abs = Math.abs(num);
    let value = "";
    if (abs >= 1_000_000_000) value = (num / 1_000_000_000).toFixed(1) + "B";
    else if (abs >= 1_000_000) value = (num / 1_000_000).toFixed(1) + "M";
    else if (abs >= 1_000) value = (num / 1_000).toFixed(1) + "K";
    else value = String(num);
    return `${value} loops`;
}

function formatCountLabel(label, n) {
    const num = Number(n) || 0;
    return `${num.toLocaleString("en-US")} ${label}`;
}

function timeAgo(created) {
    if (!created) return "";
    // handles "2016-07-31T22:08:15.000000"
    const match = created.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})/);
    let iso = created;
    if (match) iso = match[1] + "Z";
    const t = Date.parse(iso);
    if (Number.isNaN(t)) return "";
    const diff = Date.now() - t;
    const s = Math.floor(diff / 1000);
    if (s < 60) return `${s} second${s === 1 ? "" : "s"} ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m} minute${m === 1 ? "" : "s"} ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h} hour${h === 1 ? "" : "s"} ago`;
    const d = Math.floor(h / 24);
    if (d < 7) return `${d} day${d === 1 ? "" : "s"} ago`;
    const w = Math.floor(d / 7);
    if (w < 4) return `${w} week${w === 1 ? "" : "s"} ago`;
    const mo = Math.floor(d / 30);
    if (mo < 12) return `${mo} month${mo === 1 ? "" : "s"} ago`;
    const y = Math.floor(d / 365);
    return `${y} year${y === 1 ? "" : "s"} ago`;
}

function formatFullDate(created) {
    if (!created) return "";
    const match = created.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})/);
    let iso = created;
    if (match) iso = match[1] + "Z";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleDateString("en-US", {
        month: "long",
        day: "numeric",
        year: "numeric",
    });
}

function saveHistoryEntry(post) {
    try {
        const key = "vineArchiveHistory";
        const existing = JSON.parse(localStorage.getItem(key) || "[]");
        const id = String(post.postIdStr || post.postId || "");
        if (!id) return;
        const ts = Date.now();

        const filtered = existing.filter(
            (x) => String(x.postId) !== id || String(x.userId) !== String(post.userId)
        );
        filtered.unshift({
            postId: id,
            userId: String(post.userIdStr || post.userId || ""),
            username: post.username || "",
            thumbnailUrl: post.thumbnailUrl || "",
            description:
                post.description ||
                post.descriptionPlain ||
                (post.caption && post.caption.text) ||
                "",
            loops: post.loops || post.loopCount || 0,
            created: post.created || post.created_at || "",
            watchedAt: ts,
        });
        const trimmed = filtered.slice(0, 500);
        localStorage.setItem(key, JSON.stringify(trimmed));
    } catch (e) {
        console.warn("Failed to save history", e);
    }
}

function loadHistory() {
    try {
        const key = "vineArchiveHistory";
        return JSON.parse(localStorage.getItem(key) || "[]");
    } catch {
        return [];
    }
}

function setNavActive(mode) {
    document.querySelectorAll(".nav-item").forEach((btn) => {
        btn.classList.toggle("active", btn.dataset.nav === mode);
    });
}

const SAVED_PROFILES_KEY = "vineArchiveSavedProfiles";

function loadSavedProfilesFromStorage() {
    try {
        return JSON.parse(localStorage.getItem(SAVED_PROFILES_KEY) || "[]");
    } catch {
        return [];
    }
}

function saveSavedProfilesToStorage() {
    try {
        localStorage.setItem(SAVED_PROFILES_KEY, JSON.stringify(savedProfiles));
    } catch (e) {
        console.warn("Failed to persist saved profiles", e);
    }
}

function isProfileSaved(userId) {
    return savedProfiles.some((p) => String(p.userId) === String(userId));
}

function renderSavedProfiles() {
    savedUserListEl.innerHTML = "";
    savedProfiles.forEach((p) => {
        const row = document.createElement("div");
        row.className = "user-list-item";
        row.addEventListener("click", () => {
            history.pushState(null, "", `/u/${encodeURIComponent(p.userId)}`);
            routeFromLocation();
        });

        row.innerHTML = `
      <img class="avatar" src="${p.avatarUrl || ""}" alt="">
      <div>
        <div class="user-list-name">${escapeHTML(p.username || "Unknown")}</div>
        <div class="user-list-count">saved</div>
      </div>
    `;

        savedUserListEl.appendChild(row);
    });
}

function updateFollowButtonUI() {
    if (!currentProfileInfo || !followBtn) return;
    const { userId } = currentProfileInfo;
    const saved = isProfileSaved(userId);
    followBtn.textContent = saved ? "Saved" : "Follow";
    followBtn.classList.toggle("following", saved);
}

function toggleFollowCurrentProfile() {
    if (!currentProfileInfo) return;
    const { userId, username, avatarUrl } = currentProfileInfo;
    const idStr = String(userId);

    if (isProfileSaved(idStr)) {
        savedProfiles = savedProfiles.filter(
            (p) => String(p.userId) !== idStr
        );
    } else {
        savedProfiles.unshift({
            userId: idStr,
            username: username || "",
            avatarUrl: avatarUrl || "",
        });
        // optional limit
        savedProfiles = savedProfiles.slice(0, 200);
    }

    saveSavedProfilesToStorage();
    renderSavedProfiles();
    updateFollowButtonUI();
}

function initSavedProfiles() {
    savedProfiles = loadSavedProfilesFromStorage();
    renderSavedProfiles();
    if (followBtn) {
        followBtn.addEventListener("click", () => {
            toggleFollowCurrentProfile();
        });
    }
}


// --- global state ---

let currentUserId = null;
let currentPostId = null;
let autoplay = true;

let allUsersCache = null;
let currentUserPostsCache = {}; // userId -> posts array

let savedProfiles = [];          // browser-cached follows
let usersRendered = 0;           // how many /api/users have been rendered
const USERS_PAGE_SIZE = 40;      // how many profiles per chunk
let currentProfileInfo = null;   // {userId, username, avatarUrl}

// --- DOM refs ---

const viewFeed = document.getElementById("view-feed");
const viewWatch = document.getElementById("view-watch");
const viewHistory = document.getElementById("view-history");
const feedList = document.getElementById("feed-list");
const feedTitle = document.getElementById("feed-title");
const vineCountSpan = document.getElementById("vine-count");
const userListEl = document.getElementById("user-list");
const historyList = document.getElementById("history-list");

// watch view elements
const watchVideo = document.getElementById("watch-video");
const watchTitle = document.getElementById("watch-title");
const watchLoops = document.getElementById("watch-loops");
const watchLikes = document.getElementById("watch-likes");
const watchReposts = document.getElementById("watch-reposts");
const watchComments = document.getElementById("watch-comments");
const watchAvatar = document.getElementById("watch-avatar");
const watchUsername = document.getElementById("watch-username");
const watchCreated = document.getElementById("watch-created");
const watchDescription = document.getElementById("watch-description");
const upnextList = document.getElementById("upnext-list");
const autoplayToggle = document.getElementById("autoplay-toggle");

// modal
const modal = document.getElementById("archive-modal");
const modalClose = document.getElementById("archive-modal-close");
const modalHide = document.getElementById("archive-modal-hide");

// --- views switching ---

function showView(name) {
    viewFeed.classList.remove("active");
    viewWatch.classList.remove("active");
    viewHistory.classList.remove("active");

    if (name === "feed") viewFeed.classList.add("active");
    else if (name === "watch") viewWatch.classList.add("active");
    else if (name === "history") viewHistory.classList.add("active");
}

// --- feed rendering ---

function renderFeed(posts) {
    feedList.innerHTML = "";
    posts.forEach((p) => {
        const card = document.createElement("div");
        card.className = "vine-card";
        card.addEventListener("click", () => {
            history.pushState(null, "", `/v/${encodeURIComponent(p.postId)}`);
            routeFromLocation();
        });

        const thumbUrl = p.thumbnailUrl || "";

        card.innerHTML = `
      <div class="vine-card-thumb">
        ${thumbUrl
                ? `<img loading="lazy" src="${thumbUrl}" alt="thumbnail">`
                : '<div style="height:0;padding-bottom:56.25%;background:#000;"></div>'
            }
        <span class="vine-card-duration">0:06</span>
      </div>
      <div class="vine-card-body">
        <img class="avatar" src="${(p.avatarUrl || "").replace(
                /^$/,
                ""
            )}" alt="">
        <div class="vine-card-meta">
          <h3 class="vine-card-title">${escapeHTML(
                p.description || "(no description)"
            )}</h3>
          <div class="vine-card-user">
            <span>${escapeHTML(p.username || "Unknown")}</span>
            <span>•</span>
            <span>${timeAgo(p.created)}</span>
          </div>
          <div class="vine-card-stats">
            ${formatLoops(p.loops || p.loopCount)} • ${p.likes != null ? formatCountLabel("likes", p.likes) : ""
            }
          </div>
        </div>
      </div>
    `;
        feedList.appendChild(card);
    });
}

// --- up next rendering ---

function renderUpNext(posts, currentId) {
    upnextList.innerHTML = "";
    posts.forEach((p) => {
        const pid = String(p.postIdStr || p.postId || "");
        if (!pid || pid === currentId) return;

        const item = document.createElement("div");
        item.className = "upnext-item";
        item.addEventListener("click", () => {
            history.pushState(null, "", `/v/${encodeURIComponent(pid)}`);
            routeFromLocation();
        });
        const thumbUrl = p.thumbnailUrl || "";

        item.innerHTML = `
      <div class="upnext-thumb">
        ${thumbUrl
                ? `<img loading="lazy" src="${thumbUrl}" alt="thumbnail">`
                : ""
            }
      </div>
      <div class="upnext-meta">
        <div class="upnext-title">${escapeHTML(
                p.description || "(no description)"
            )}</div>
        <div class="upnext-user">${escapeHTML(p.username || "Unknown")}</div>
        <div class="upnext-stats">${formatLoops(
                p.loops || p.loopCount
            )}</div>
      </div>
    `;
        upnextList.appendChild(item);
    });
}

// --- user list rendering ---

function renderUserList(users) {
    userListEl.innerHTML = "";
    users.forEach((u) => {
        const row = document.createElement("div");
        row.className = "user-list-item";
        row.addEventListener("click", () => {
            history.pushState(null, "", `/u/${encodeURIComponent(u.userId)}`);
            routeFromLocation();
        });

        row.innerHTML = `
      <div class="avatar"></div>
      <div>
        <div class="user-list-name">${escapeHTML(u.username || "Unknown")}</div>
        <div class="user-list-count">${(u.postCount || 0).toLocaleString(
            "en-US"
        )} vines</div>
      </div>
    `;
        userListEl.appendChild(row);
    });
}

function appendUserRow(u) {
    const row = document.createElement("div");
    row.className = "user-list-item";
    row.addEventListener("click", () => {
        history.pushState(null, "", `/u/${encodeURIComponent(u.userId)}`);
        routeFromLocation();
    });

    row.innerHTML = `
    <div class="avatar"></div>
    <div>
      <div class="user-list-name">${escapeHTML(u.username || "Unknown")}</div>
      <div class="user-list-count">${(u.postCount || 0).toLocaleString(
        "en-US"
    )} vines</div>
    </div>
  `;

    userListEl.appendChild(row);
}

function renderUserChunk() {
    if (!allUsersCache) return;
    let count = 0;
    while (
        usersRendered < allUsersCache.length &&
        count < USERS_PAGE_SIZE
    ) {
        appendUserRow(allUsersCache[usersRendered]);
        usersRendered++;
        count++;
    }
}

function onUserListScroll() {
    if (
        userListEl.scrollTop + userListEl.clientHeight >=
        userListEl.scrollHeight - 40
    ) {
        renderUserChunk();
    }
}



// --- main load functions ---

async function loadUsersOnce() {
    if (allUsersCache) return allUsersCache;
    const users = await fetchJSON("/api/users");
    allUsersCache = users;
    renderUserList(users);
    return users;
}

async function loadFeed() {
    setNavActive("home");
    showView("feed");
    feedTitle.textContent = "Home";
    const posts = await fetchJSON("/api/feed?limit=120");
    renderFeed(posts);
}

async function loadHistoryView() {
    setNavActive("history");
    showView("history");
    const history = loadHistory();
    historyList.innerHTML = "";
    history.forEach((p) => {
        const card = document.createElement("div");
        card.className = "vine-card";
        card.addEventListener("click", () => {
            historyPushToWatch(p.postId);
        });
        const thumbUrl = p.thumbnailUrl || "";

        card.innerHTML = `
      <div class="vine-card-thumb">
        ${thumbUrl
                ? `<img loading="lazy" src="${thumbUrl}" alt="thumbnail">`
                : ""
            }
        <span class="vine-card-duration">0:06</span>
      </div>
      <div class="vine-card-body">
        <img class="avatar" src="" alt="">
        <div class="vine-card-meta">
          <h3 class="vine-card-title">${escapeHTML(
                p.description || "(no description)"
            )}</h3>
          <div class="vine-card-user">
            <span>${escapeHTML(p.username || "Unknown")}</span>
            <span>•</span>
            <span>${timeAgo(p.created)}</span>
          </div>
          <div class="vine-card-stats">
            ${formatLoops(p.loops)} • watched
          </div>
        </div>
      </div>
    `;
        historyList.appendChild(card);
    });
}

function historyPushToWatch(postId) {
    history.pushState(null, "", `/v/${encodeURIComponent(postId)}`);
    routeFromLocation();
}

async function loadUserPosts(userId) {
    if (currentUserPostsCache[userId]) return currentUserPostsCache[userId];
    const posts = await fetchJSON(`/api/users/${encodeURIComponent(userId)}/posts`);
    currentUserPostsCache[userId] = posts;
    return posts;
}

async function loadWatchByPostId(postId) {
    try {
        const data = await fetchJSON(
            `/api/watch/${encodeURIComponent(postId)}`
        );

        const userId = String(data.userIdStr || data.userId || "");
        const username = data.username || "";
        const resolvedPostId = String(data.postIdStr || data.postId || postId);

        if (!userId) {
            await loadFeed();
            return;
        }

        currentUserId = userId;
        currentPostId = resolvedPostId;

        setNavActive("");
        showView("watch");

        // main video
        watchVideo.src = data.videoUrl || data.videoLowURL || data.videoDashUrl || "";
        watchTitle.textContent = data.description || "(no description)";
        watchLoops.textContent = formatLoops(data.loops || data.loopCount);
        watchLikes.textContent = formatCountLabel("likes", data.likes || data.likeCount);
        watchReposts.textContent = formatCountLabel("reposts", data.reposts || data.repostCount);
        watchComments.textContent = formatCountLabel("comments", data.comments || data.commentCount);

        const avatar = data.avatarUrl || "";
        currentProfileInfo = {
            userId,
            username,
            avatarUrl: avatar,
        };
        updateFollowButtonUI();
        if (avatar) {
            watchAvatar.src = avatar;
            watchAvatar.style.display = "block";
        } else {
            watchAvatar.style.display = "none";
        }
        watchUsername.textContent = username || "Unknown";
        watchCreated.textContent = formatFullDate(
            data.created || data.created_at || ""
        );
        watchDescription.textContent =
            data.description ||
            data.descriptionPlain ||
            (data.caption && data.caption.text) ||
            "";

        // save to history
        saveHistoryEntry(data);

        // up next = this user's posts
        const userPosts = await loadUserPosts(userId);
        renderUpNext(userPosts, resolvedPostId);

        watchVideo.onended = () => {
            if (!autoplay) return;
            const next = userPosts.find((p) => {
                const pid = String(p.postIdStr || p.postId || "");
                return pid && pid !== resolvedPostId;
            });
            if (next) {
                const nextId = String(next.postIdStr || next.postId);
                history.pushState(null, "", `/v/${encodeURIComponent(nextId)}`);
                routeFromLocation();
            }
        };
    } catch (e) {
        console.error("Failed to load watch", e);
        await loadFeed();
    }
}

// --- search ---

async function performSearch(query) {
    if (!query.trim()) {
        await loadFeed();
        return;
    }
    setNavActive("");
    showView("feed");
    feedTitle.textContent = `Results for "${query}"`;
    const results = await fetchJSON(`/api/search?q=${encodeURIComponent(query)}`);
    renderFeed(results);
}

// --- vine count ---

async function loadVineCount() {
    try {
        // Try /api/stats if you added it
        const statsRes = await fetch("/api/stats");
        if (statsRes.ok) {
            const stats = await statsRes.json();
            if (typeof stats.totalPosts === "number") {
                vineCountSpan.textContent = stats.totalPosts.toLocaleString("en-US");
                return;
            }
        }
    } catch {
        // ignore, fallback below
    }

    try {
        // fallback: sum postCount from /api/users
        const users = await fetchJSON("/api/users");
        const total = users.reduce(
            (sum, u) => sum + (Number(u.postCount) || 0),
            0
        );
        vineCountSpan.textContent = total.toLocaleString("en-US");
    } catch {
        vineCountSpan.textContent = "–";
    }
}

// --- router ---

function routeFromLocation() {
    const path = window.location.pathname || "/";

    // /v/:id
    const vineMatch = path.match(/^\/v\/([^/]+)$/);
    if (vineMatch) {
        const postId = decodeURIComponent(vineMatch[1]);
        loadWatchByPostId(postId);
        return;
    }

    // /u/:id or /u/name
    const userMatch = path.match(/^\/u\/([^/]+)$/);
    if (userMatch) {
        const idOrName = decodeURIComponent(userMatch[1]);
        // simple: treat as userId (you can extend to vanity if you want)
        loadUserProfile(idOrName);
        return;
    }

    // /search?q=...
    const params = new URLSearchParams(window.location.search);
    const q = params.get("q");
    if (path === "/search" && q) {
        performSearch(q);
        return;
    }

    // default: home
    loadFeed();
}

async function loadUserProfile(userId) {
    setNavActive("");
    showView("feed");
    feedTitle.textContent = "User profile";

    const posts = await loadUserPosts(userId);
    renderFeed(posts);
}

// --- modal ---

function initArchiveModal() {
    try {
        const key = "vineArchiveHideModal";
        const hide = localStorage.getItem(key) === "1";
        if (hide) {
            modal.classList.add("hidden");
        } else {
            modal.classList.remove("hidden");
        }

        modalClose.addEventListener("click", () => {
            if (modalHide.checked) {
                localStorage.setItem(key, "1");
            }
            modal.classList.add("hidden");
        });
    } catch {
        modal.classList.add("hidden");
    }
}

// --- utils ---

function escapeHTML(str) {
    return String(str || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

// --- init ---

function initNavButtons() {
    document.querySelectorAll(".nav-item").forEach((btn) => {
        btn.addEventListener("click", () => {
            const mode = btn.dataset.nav;
            if (mode === "home") {
                history.pushState(null, "", "/");
                routeFromLocation();
            } else if (mode === "history") {
                loadHistoryView();
            }
        });
    });
}

function initSearch() {
    const form = document.getElementById("search-form");
    const input = document.getElementById("search-input");
    form.addEventListener("submit", (e) => {
        e.preventDefault();
        const q = input.value.trim();
        if (!q) {
            history.pushState(null, "", "/");
            routeFromLocation();
            return;
        }
        history.pushState(
            null,
            "",
            `/search?q=${encodeURIComponent(q)}`
        );
        routeFromLocation();
    });
}

function initAutoplay() {
    autoplayToggle.addEventListener("change", () => {
        autoplay = autoplayToggle.checked;
    });
}

window.addEventListener("popstate", () => {
    routeFromLocation();
});

window.addEventListener("DOMContentLoaded", async () => {
    initArchiveModal();
    initNavButtons();
    initSearch();
    initAutoplay();

    loadVineCount();
    loadUsersOnce();
    routeFromLocation();
    userListEl.addEventListener("scroll", onUserListScroll);

});
