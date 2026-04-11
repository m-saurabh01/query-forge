const messagesEl = document.getElementById("messages");
const form = document.getElementById("query-form");
const input = document.getElementById("query-input");
const sendBtn = document.getElementById("send-btn");
const sidebar = document.getElementById("sidebar");
const sidebarToggle = document.getElementById("sidebar-toggle");
const sidebarOpenBtn = document.getElementById("sidebar-open-btn");
const newChatBtn = document.getElementById("new-chat-btn");
const chatHistory = document.getElementById("chat-history");
const welcomeScreen = document.getElementById("welcome-screen");
const chatContainer = document.getElementById("chat-container");
const themeToggle = document.getElementById("theme-toggle");
const themeIconDark = document.getElementById("theme-icon-dark");
const themeIconLight = document.getElementById("theme-icon-light");
const themeLabel = document.getElementById("theme-label");

// Chat sessions stored in memory
let chats = [];
let activeChatId = null;

function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
}

// ── Theme toggle ──
function getStoredTheme() {
    return localStorage.getItem("queryforge-theme") || "dark";
}

function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("queryforge-theme", theme);
    if (theme === "dark") {
        themeIconDark.classList.remove("hidden");
        themeIconLight.classList.add("hidden");
        themeLabel.textContent = "Light mode";
    } else {
        themeIconDark.classList.add("hidden");
        themeIconLight.classList.remove("hidden");
        themeLabel.textContent = "Dark mode";
    }
}

applyTheme(getStoredTheme());

themeToggle.addEventListener("click", () => {
    const current = document.documentElement.getAttribute("data-theme");
    applyTheme(current === "dark" ? "light" : "dark");
});

// ── Sidebar toggle ──
function toggleSidebar() {
    sidebar.classList.toggle("collapsed");
    sidebarOpenBtn.classList.toggle("hidden", !sidebar.classList.contains("collapsed"));
}

sidebarToggle.addEventListener("click", toggleSidebar);
sidebarOpenBtn.addEventListener("click", toggleSidebar);

// ── Enable/disable send button based on input ──
input.addEventListener("input", () => {
    sendBtn.disabled = !input.value.trim();
});

// ── Chat history rendering ──
function renderChatHistory() {
    chatHistory.innerHTML = "";
    if (chats.length === 0) return;

    const label = document.createElement("div");
    label.className = "chat-history-label";
    label.textContent = "Recent";
    chatHistory.appendChild(label);

    for (const chat of [...chats].reverse()) {
        const item = document.createElement("div");
        item.className = "chat-history-item" + (chat.id === activeChatId ? " active" : "");
        item.textContent = chat.title || "New chat";
        item.addEventListener("click", () => switchChat(chat.id));
        chatHistory.appendChild(item);
    }
}

function switchChat(chatId) {
    activeChatId = chatId;
    const chat = chats.find(c => c.id === chatId);
    if (!chat) return;

    messagesEl.innerHTML = "";
    for (const msg of chat.messages) {
        if (msg.role === "user") {
            addUserMessage(msg.content, false);
        } else {
            addBotMessage(msg.content, false);
        }
    }

    welcomeScreen.classList.add("hidden");
    chatContainer.classList.remove("hidden");
    renderChatHistory();
    scrollToBottom();
}

function newChat() {
    activeChatId = null;
    messagesEl.innerHTML = "";
    welcomeScreen.classList.remove("hidden");
    chatContainer.classList.add("hidden");
    renderChatHistory();
    input.value = "";
    sendBtn.disabled = true;
    input.focus();
}

newChatBtn.addEventListener("click", newChat);

// ── Scroll ──
function scrollToBottom() {
    chatContainer.scrollTop = chatContainer.scrollHeight;
}

// ── Messages ──
function addUserMessage(text, save = true) {
    const div = document.createElement("div");
    div.className = "message user";
    div.textContent = text;
    messagesEl.appendChild(div);
    scrollToBottom();

    if (save && activeChatId) {
        const chat = chats.find(c => c.id === activeChatId);
        if (chat) chat.messages.push({ role: "user", content: text });
    }
}

function addLoadingMessage() {
    const div = document.createElement("div");
    div.className = "message loading";
    div.id = "loading-msg";
    div.innerHTML = '<div class="dot-flashing"><span></span><span></span><span></span></div>';
    messagesEl.appendChild(div);
    scrollToBottom();
    return div;
}

function removeLoadingMessage() {
    const el = document.getElementById("loading-msg");
    if (el) el.remove();
}

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

function buildResultTable(data) {
    if (!data || !data.columns || data.columns.length === 0) {
        return '<p style="color: var(--text-secondary); font-size: 0.85rem;">No results returned.</p>';
    }

    let html = '<div class="results-table-wrapper"><table class="results-table"><thead><tr>';
    for (const col of data.columns) {
        html += `<th>${escapeHtml(col)}</th>`;
    }
    html += "</tr></thead><tbody>";

    if (data.rows.length === 0) {
        html += `<tr><td colspan="${data.columns.length}" style="text-align:center; color: var(--text-secondary);">No rows found</td></tr>`;
    } else {
        for (const row of data.rows) {
            html += "<tr>";
            for (const cell of row) {
                html += `<td>${escapeHtml(cell === null ? "NULL" : String(cell))}</td>`;
            }
            html += "</tr>";
        }
    }

    html += "</tbody></table></div>";
    return html;
}

function addBotMessage(result, save = true) {
    const div = document.createElement("div");
    div.className = "message bot";

    if (result.error) {
        div.classList.add("error");
        let html = `<strong>Error:</strong> ${escapeHtml(result.error)}`;
        if (result.sql) {
            html += `<div class="sql-block"><div class="sql-label">Generated SQL</div><pre class="sql-code">${escapeHtml(result.sql)}</pre></div>`;
        }
        div.innerHTML = html;
    } else {
        let html = "";

        if (result.sql) {
            html += `<div class="sql-block"><div class="sql-label">Generated SQL</div><pre class="sql-code">${escapeHtml(result.sql)}</pre></div>`;
        }

        html += buildResultTable(result.data);

        if (result.explanation) {
            html += `<div class="explanation">${escapeHtml(result.explanation)}</div>`;
        }

        div.innerHTML = html;
    }

    messagesEl.appendChild(div);
    scrollToBottom();

    if (save && activeChatId) {
        const chat = chats.find(c => c.id === activeChatId);
        if (chat) chat.messages.push({ role: "bot", content: result });
    }
}

async function handleSubmit(e) {
    e.preventDefault();
    const query = input.value.trim();
    if (!query) return;

    // If no active chat, create one
    if (!activeChatId) {
        const chat = {
            id: generateId(),
            title: query.length > 40 ? query.slice(0, 40) + "…" : query,
            messages: [],
        };
        chats.push(chat);
        activeChatId = chat.id;
        welcomeScreen.classList.add("hidden");
        chatContainer.classList.remove("hidden");
    }

    addUserMessage(query);
    input.value = "";
    sendBtn.disabled = true;
    input.disabled = true;

    addLoadingMessage();
    renderChatHistory();

    try {
        const response = await fetch("/api/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query }),
        });

        const result = await response.json();
        removeLoadingMessage();
        addBotMessage(result);
    } catch (err) {
        removeLoadingMessage();
        addBotMessage({ error: "Network error: Could not reach the server.", sql: null, data: null, explanation: null });
    } finally {
        input.disabled = false;
        input.focus();
    }
}

form.addEventListener("submit", handleSubmit);

input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        form.dispatchEvent(new Event("submit"));
    }
});

// Initialize
renderChatHistory();
