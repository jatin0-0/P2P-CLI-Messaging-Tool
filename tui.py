"""
tui.py — Textual-powered TUI for the P2P chat client.

Layout
------
  ┌─────────────────────────────────────┐
  │  ● P2P CHAT   alice  [bob ●2][carol]│  ← Header + peer tabs
  ├─────────────────────────────────────┤
  │                                     │
  │  10:42  bob  hey, can you see this? │  ← Message log (scrollable)
  │  10:42  you  yeah! loud and clear   │
  │                                     │
  ├─────────────────────────────────────┤
  │  [+ Add peer]                       │  ← Sidebar / controls
  │  bob    ● online  (lan)             │
  │  carol  ◌ relay                     │
  ├─────────────────────────────────────┤
  │  you>  ________________________________│  ← Input bar
  └─────────────────────────────────────┘

Features
--------
  • One tab per peer — switch with mouse or Ctrl+number
  • Unread badge on inactive tabs
  • OS notification on incoming message (when app is not focused)
  • Message history loaded from SQLite on tab open
  • Status indicators: connecting / online (lan/holepunch) / relay / offline
  • /add <peer_id>   — connect to a new peer
  • /quit or Ctrl+C  — exit
"""

import asyncio
from datetime import datetime
from textual.app import App, ComposeResult
from textual.widgets import (
    Header, Footer, Input, Label, ListView,
    ListItem, Static, TabbedContent, TabPane, Log
)
from textual.containers import Horizontal, Vertical, ScrollableContainer
from textual.reactive import reactive
from textual.message import Message
from textual import on, work

from peer_manager import PeerManager
from peer_worker import MessageReceived, PeerStatusChanged
from storage import Storage
from config import LOCAL_UDP_PORT, MAX_HISTORY_DISPLAY

try:
    from plyer import notification as _plyer_notif
    _PLYER = True
except ImportError:
    _PLYER = False


# ── Custom Textual messages ───────────────────────────────────────────────────

class IncomingMessage(Message):
    def __init__(self, peer_id: str, body: str, ts: str):
        super().__init__()
        self.peer_id = peer_id
        self.body = body
        self.ts = ts

class StatusUpdate(Message):
    def __init__(self, peer_id: str, status: str, path: str):
        super().__init__()
        self.peer_id = peer_id
        self.status = status
        self.path = path


# ── Widgets ───────────────────────────────────────────────────────────────────

STATUS_ICON = {
    "connecting": "◌",
    "online":     "●",
    "relay":      "◈",
    "offline":    "○",
}

PATH_COLOR = {
    "lan":        "green",
    "holepunch":  "cyan",
    "relay":      "yellow",
    "none":       "red",
}


class PeerRow(Static):
    """Sidebar row showing one peer's status."""

    def __init__(self, peer_id: str):
        super().__init__()
        self.peer_id = peer_id
        self._status = "connecting"
        self._path   = "none"

    def compose(self) -> ComposeResult:
        yield Label(f"◌  {self.peer_id}", id=f"label-{self.peer_id}")

    def update_status(self, status: str, path: str):
        self._status = status
        self._path   = path
        icon  = STATUS_ICON.get(status, "○")
        color = PATH_COLOR.get(path, "white")
        label = self.query_one(f"#label-{self.peer_id}", Label)
        label.update(f"[{color}]{icon}[/] {self.peer_id}  [{color}]{path}[/]")


class ChatPane(ScrollableContainer):
    """Scrollable message log for one peer."""

    def __init__(self, peer_id: str, **kwargs):
        super().__init__(**kwargs)
        self.peer_id = peer_id

    def compose(self) -> ComposeResult:
        yield Log(id=f"log-{self.peer_id}", highlight=True, markup=True)

    def append_message(self, direction: str, body: str, ts: str, my_id: str):
        log = self.query_one(f"#log-{self.peer_id}", Log)
        time_str = ts[11:16] if len(ts) >= 16 else ts  # HH:MM
        if direction == "incoming":
            log.write_line(
                f"[dim]{time_str}[/]  [bold cyan]{self.peer_id}[/]  {body}"
            )
        else:
            log.write_line(
                f"[dim]{time_str}[/]  [bold white]you[/]  [dim]{body}[/]"
            )

    def append_system(self, text: str):
        log = self.query_one(f"#log-{self.peer_id}", Log)
        log.write_line(f"[dim italic]{text}[/]")


# ── Main App ──────────────────────────────────────────────────────────────────

class P2PChat(App):
    """The top-level Textual app."""

    CSS = """
    Screen {
        background: $surface;
    }

    Header {
        background: $panel;
        color: $text;
        text-style: bold;
        height: 3;
    }

    #sidebar {
        width: 24;
        background: $panel;
        border-right: solid $primary-darken-2;
        padding: 1 1;
    }

    #sidebar-title {
        color: $text-muted;
        text-style: bold;
        margin-bottom: 1;
    }

    PeerRow {
        height: 2;
        padding: 0 1;
    }

    PeerRow:hover {
        background: $primary-darken-3;
    }

    #add-peer-btn {
        margin-top: 1;
        color: $accent;
        text-style: bold;
    }

    #main-area {
        background: $surface;
    }

    TabbedContent {
        height: 1fr;
    }

    ChatPane {
        padding: 1 2;
        height: 1fr;
    }

    Log {
        background: $surface;
        color: $text;
        scrollbar-gutter: stable;
    }

    #input-bar {
        height: 3;
        background: $panel;
        border-top: solid $primary-darken-2;
        padding: 0 2;
        align: left middle;
    }

    #message-input {
        width: 1fr;
        background: $surface;
        border: none;
        color: $text;
    }

    #status-bar {
        height: 1;
        background: $panel-darken-1;
        color: $text-muted;
        padding: 0 2;
    }

    .unread-badge {
        color: $warning;
    }
    """

    TITLE = "P2P Chat"
    BINDINGS = [
        ("ctrl+c", "quit", "Quit"),
        ("ctrl+n", "focus_input", "Focus input"),
    ]

    def __init__(self, my_id: str, peers: list[str], udp_port: int = LOCAL_UDP_PORT):
        super().__init__()
        self.my_id     = my_id
        self.peers     = list(peers)
        self.udp_port  = udp_port

        self._manager: PeerManager | None = None
        self._storage: Storage | None = None

        # peer_id → unread count (when tab is not active)
        self._unread: dict[str, int] = {p: 0 for p in peers}
        self._active_peer: str | None = peers[0] if peers else None

        # peer_id → ChatPane reference
        self._panes: dict[str, ChatPane] = {}
        self._rows:  dict[str, PeerRow]  = {}

    # ── Composition ───────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Horizontal():
            # Sidebar
            with Vertical(id="sidebar"):
                yield Static("PEERS", id="sidebar-title")
                for peer in self.peers:
                    row = PeerRow(peer)
                    self._rows[peer] = row
                    yield row
                yield Static("+ /add <id>", id="add-peer-btn")

            # Main chat area
            with Vertical(id="main-area"):
                with TabbedContent(*[f"{p}" for p in self.peers], id="tabs"):
                    for peer in self.peers:
                        with TabPane(peer, id=f"tab-{peer}"):
                            pane = ChatPane(peer, id=f"pane-{peer}")
                            self._panes[peer] = pane
                            yield pane

                with Horizontal(id="input-bar"):
                    yield Input(
                        placeholder="  type a message  ·  /add <peer>  ·  /quit",
                        id="message-input"
                    )

        yield Static(
            f"  {self.my_id}  ·  UDP :{self.udp_port}  ·  Ctrl+C to quit",
            id="status-bar"
        )
        yield Footer()

    # ── Startup ───────────────────────────────────────────────────────────────

    async def on_mount(self) -> None:
        self._storage = await Storage.connect()
        self._manager = await PeerManager.create(
            my_id    = self.my_id,
            udp_port = self.udp_port,
            on_event = self._handle_worker_event,
            storage  = self._storage,
        )

        # Load history and connect to initial peers
        for peer in self.peers:
            await self._open_peer(peer)

        self.query_one("#message-input", Input).focus()

    async def _open_peer(self, peer_id: str):
        """Load history for a peer and start their worker."""
        history = await self._storage.get_history(peer_id, MAX_HISTORY_DISPLAY)
        pane = self._panes.get(peer_id)
        if pane and history:
            for msg in history:
                pane.append_message(msg["direction"], msg["body"], msg["ts"], self.my_id)
            pane.append_system("── history ──")

        await self._manager.add_peer(peer_id)

    # ── Worker event handler ──────────────────────────────────────────────────

    async def _handle_worker_event(self, event):
        """Receives events from PeerWorker coroutines (running in same loop)."""
        if isinstance(event, MessageReceived):
            self.post_message(IncomingMessage(event.peer_id, event.body, event.ts))
        elif isinstance(event, PeerStatusChanged):
            self.post_message(StatusUpdate(event.peer_id, event.status, event.path))

    # ── Message handlers ──────────────────────────────────────────────────────

    @on(IncomingMessage)
    def on_incoming_message(self, msg: IncomingMessage):
        pane = self._panes.get(msg.peer_id)
        if pane:
            pane.append_message("incoming", msg.body, msg.ts, self.my_id)

        # Unread badge if this tab isn't active
        if msg.peer_id != self._active_peer:
            self._unread[msg.peer_id] = self._unread.get(msg.peer_id, 0) + 1
            self._refresh_tab_label(msg.peer_id)

        # OS notification
        if _PLYER:
            try:
                _plyer_notif.notify(
                    title=f"Message from {msg.peer_id}",
                    message=msg.body[:80],
                    app_name="P2P Chat",
                    timeout=4,
                )
            except Exception:
                pass

    @on(StatusUpdate)
    def on_status_update(self, msg: StatusUpdate):
        row = self._rows.get(msg.peer_id)
        if row:
            row.update_status(msg.status, msg.path)

        pane = self._panes.get(msg.peer_id)
        if pane:
            icons = {"connecting": "connecting…", "online": f"connected via {msg.path}",
                     "relay": "connected via relay", "offline": "offline — reconnecting…"}
            pane.append_system(icons.get(msg.status, msg.status))

    @on(Input.Submitted, "#message-input")
    async def on_message_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        if not text:
            return
        self.query_one("#message-input", Input).value = ""

        # Commands
        if text.startswith("/quit"):
            await self.action_quit()
            return

        if text.startswith("/add "):
            peer_id = text.split(" ", 1)[1].strip()
            await self._add_peer_dynamic(peer_id)
            return

        # Send to active peer
        if self._active_peer:
            await self._manager.send_to(self._active_peer, text)
            ts = datetime.now().isoformat(timespec="seconds")
            pane = self._panes.get(self._active_peer)
            if pane:
                pane.append_message("outgoing", text, ts, self.my_id)

    @on(TabbedContent.TabActivated)
    def on_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        # Extract peer_id from tab id "tab-<peer_id>"
        tab_id = str(event.tab.id)
        if tab_id.startswith("tab-"):
            peer_id = tab_id[4:]
            self._active_peer = peer_id
            self._unread[peer_id] = 0
            self._refresh_tab_label(peer_id)

    # ── Dynamic peer addition ─────────────────────────────────────────────────

    async def _add_peer_dynamic(self, peer_id: str):
        if peer_id in self._panes:
            pane = self._panes[peer_id]
            pane.append_system(f"Already connected to {peer_id}")
            return

        self._unread[peer_id] = 0

        # Add sidebar row
        row = PeerRow(peer_id)
        self._rows[peer_id] = row
        sidebar = self.query_one("#sidebar", Vertical)
        await sidebar.mount(row, before=self.query_one("#add-peer-btn", Static))

        # Add tab pane
        tabs = self.query_one("#tabs", TabbedContent)
        pane = ChatPane(peer_id, id=f"pane-{peer_id}")
        self._panes[peer_id] = pane
        await tabs.add_pane(TabPane(peer_id, pane, id=f"tab-{peer_id}"))

        self.peers.append(peer_id)
        await self._open_peer(peer_id)
        pane.append_system(f"Connecting to {peer_id}…")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _refresh_tab_label(self, peer_id: str):
        count = self._unread.get(peer_id, 0)
        label = f"{peer_id} [{count}]" if count > 0 else peer_id
        try:
            tab = self.query_one(f"#tab-{peer_id}")
            tab.label = label
        except Exception:
            pass

    def action_focus_input(self):
        self.query_one("#message-input", Input).focus()

    async def action_quit(self):
        if self._manager:
            self._manager.close()
        if self._storage:
            await self._storage.close()
        self.exit()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    import sys

    if len(sys.argv) < 3:
        print("Usage: python tui.py <my_id> <peer1> [peer2 peer3 ...] [--port PORT]")
        print("Example: python tui.py alice bob")
        print("Example: python tui.py alice bob carol --port 9000")
        sys.exit(1)

    args = sys.argv[1:]
    port = LOCAL_UDP_PORT

    if "--port" in args:
        idx  = args.index("--port")
        port = int(args[idx + 1])
        args = args[:idx] + args[idx + 2:]

    my_id = args[0]
    peers = args[1:]

    app = P2PChat(my_id=my_id, peers=peers, udp_port=port)
    app.run()


if __name__ == "__main__":
    main()