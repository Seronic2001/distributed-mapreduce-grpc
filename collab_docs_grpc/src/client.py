#!/usr/bin/env python3
"""
Document client CLI — collaborative editing over gRPC (Section 3 Q1).

Interactive, colorful developer CLI with real-time push update streams,
tab auto-completion, rich status dashboards, and full backward compatibility.

Usage:
    python3 client.py [server_address] [options]

Options:
    -i, --interactive   Force rich interactive TUI mode
    -p, --plain         Force plain text scripted mode
    -h, --help          Show usage help
"""

import atexit
import os
import shlex
import shutil
import sys
import threading
import time

try:
    import readline
except ImportError:
    readline = None

import grpc
from rich.box import ROUNDED, SIMPLE
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import docs_pb2
import docs_pb2_grpc


def error_text(error):
    return error.details() if error.details() else str(error.code())


LEGACY_HELP = """options:
  1. Create Document      create <name> "<content>"
  2. Open Document        open <name>
  3. Edit Document        edit <name> <position> "<text>"
  4. Subscribe to Updates subscribe <name>
     (stop updates)       unsubscribe <name>
  5. Exit                 exit"""


class Subscriber(threading.Thread):
    """Listens for streaming DocumentUpdates on a background thread."""

    daemon = True

    def __init__(self, name, call, client):
        super().__init__(name="subscriber-%s" % name)
        self.name = name
        self.call = call
        self.client = client

    def run(self):
        try:
            for update in self.call:
                self.client.on_update(update)
        except grpc.RpcError as error:
            if error.code() not in (grpc.StatusCode.CANCELLED,):
                self.client.on_sub_error(self.name, error)


class DocumentClient:
    COMMANDS = [
        "create", "open", "edit", "subscribe", "unsubscribe",
        "status", "info", "list", "ls", "clear", "cls", "help", "exit", "quit"
    ]

    def __init__(self, address, interactive=False):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.stub = docs_pb2_grpc.DocumentServiceStub(self.channel)
        self.client_id = (int.from_bytes(os.urandom(2), "little") % 9000) + 1000
        self.subscriptions = {}         # name -> (call, Subscriber)
        self.interactive = interactive
        self.console = Console(highlight=False) if interactive else None
        self.print_lock = threading.Lock()
        # Session state for interactive UX
        self.known_docs = set()
        self.docs_cache = {}           # name -> {"version": int, "content": str}
        self.sub_stats = {}            # name -> {"updates": int, "start_time": float, "last_v": int}
        self.active_doc = None
        self.stats = {"commands": 0, "edits": 0, "updates": 0}

    @property
    def term_width(self):
        try:
            return shutil.get_terminal_size(fallback=(80, 24)).columns
        except Exception:
            return 80

    # -------------------------------------------------------- RPC Handlers --

    def create(self, name, content=""):
        self.stats["commands"] += 1
        reply = self.stub.CreateDocument(
            docs_pb2.CreateDocumentRequest(name=name, initial_content=content))
        self.known_docs.add(name)
        self.docs_cache[name] = {"version": reply.version, "content": content}
        self.active_doc = name

        if not self.interactive:
            print("[Client] Document %s created (v%d)." % (reply.name, reply.version))
            return

        with self.print_lock:
            self.console.print(
                f"[bold green]✔ [Client][/bold green] Document "
                f"[bold cyan]'{reply.name}'[/bold cyan] created (v{reply.version}).")
            if content:
                c_title = f"📄 [bold cyan]{reply.name}[/bold cyan] [dim](v{reply.version})[/dim]" if self.term_width < 75 else f"📄 [bold cyan]{reply.name}[/bold cyan] [dim](v{reply.version} • {len(content)} chars)[/dim]"
                panel = Panel(
                    Text(content),
                    title=c_title,
                    border_style="green",
                    box=ROUNDED,
                    padding=(0, 1),
                )
                self.console.print(panel)

    def open(self, name):
        self.stats["commands"] += 1
        reply = self.stub.GetDocument(docs_pb2.GetDocumentRequest(name=name))
        self.known_docs.add(name)
        self.docs_cache[name] = {"version": reply.version, "content": reply.content}
        self.active_doc = name

        if not self.interactive:
            print("[Client] %s" % reply.content)
            return

        with self.print_lock:
            if self.term_width < 75:
                self.console.print(
                    f"[bold cyan]📄 [Client][/bold cyan] [bold white]{name}[/bold white] [dim](v{reply.version})[/dim]")
            else:
                self.console.print(
                    f"[bold cyan]📄 [Client][/bold cyan] Document "
                    f"[bold white]{name}[/bold white] [dim]• version {reply.version} • {len(reply.content)} characters[/dim]")
            panel = Panel(
                Text(reply.content) if reply.content else Text("[Empty Document]", style="dim italic"),
                title=f"📄 [bold cyan]{name}[/bold cyan] [dim](v{reply.version})[/dim]",
                border_style="cyan",
                box=ROUNDED,
                padding=(0, 1),
            )
            self.console.print(panel)

    def edit(self, name, position, text):
        self.stats["commands"] += 1
        self.stats["edits"] += 1
        old_content = self.docs_cache.get(name, {}).get("content", "")
        reply = self.stub.EditDocument(
            docs_pb2.EditDocumentRequest(
                name=name, position=position, text=text, edited_by=self.client_id))
        self.known_docs.add(name)
        self.docs_cache[name] = {"version": reply.version, "content": reply.content}
        self.active_doc = name

        if not self.interactive:
            print("[Client] Edited %s -> v%d: %s"
                  % (reply.name, reply.version, reply.content))
            return

        with self.print_lock:
            if self.term_width < 75:
                self.console.print(
                    f"[bold green]✔ [Client][/bold green] Edited [bold cyan]{reply.name}[/bold cyan] ➔ [bold yellow]v{reply.version}[/bold yellow]")
            else:
                self.console.print(
                    f"[bold green]✔ [Client][/bold green] Edited [bold cyan]{reply.name}[/bold cyan] ➔ [bold yellow]v{reply.version}[/bold yellow] "
                    f"[dim](inserted {len(text)} chars at offset {position})[/dim]")

            # Highlight the inserted text in preview
            preview = Text()
            if 0 <= position <= len(old_content):
                preview.append(reply.content[:position])
                preview.append(text, style="bold green underline")
                preview.append(reply.content[position + len(text):])
            else:
                preview.append(reply.content)

            e_title = f"📝 [bold cyan]{reply.name}[/bold cyan] [dim](v{reply.version})[/dim]" if self.term_width < 75 else f"📝 [bold cyan]{reply.name}[/bold cyan] [dim](v{reply.version} • {len(reply.content)} chars)[/dim]"
            panel = Panel(
                preview if reply.content else Text("[Empty Document]", style="dim italic"),
                title=e_title,
                border_style="green",
                box=ROUNDED,
                padding=(0, 1),
            )
            self.console.print(panel)

    def subscribe(self, name):
        self.stats["commands"] += 1
        if name in self.subscriptions:
            if not self.interactive:
                print("[Client] Already subscribed to %s." % name)
            else:
                with self.print_lock:
                    self.console.print(f"[bold yellow]⚠ [Client][/bold yellow] Already subscribed to [bold cyan]{name}[/bold cyan].")
            return

        call = self.stub.SubscribeToUpdates(docs_pb2.UpdateRequest(name=name))
        subscriber = Subscriber(name, call, self)
        subscriber.start()
        self.subscriptions[name] = (call, subscriber)
        self.sub_stats[name] = {"updates": 0, "start_time": time.time(), "last_v": 0}
        self.known_docs.add(name)

        if not self.interactive:
            print("[Client] Subscribed to updates for %s." % name)
        else:
            with self.print_lock:
                self.console.print(
                    f"[bold magenta]📡 [Client][/bold magenta] Subscribed to real-time updates for [bold cyan]'{name}'[/bold cyan].\n"
                    f"   [dim]Background sync active. Incoming edits will appear automatically.[/dim]")

    def unsubscribe(self, name):
        self.stats["commands"] += 1
        entry = self.subscriptions.pop(name, None)
        if entry is None:
            if not self.interactive:
                print("[Client] Not subscribed to %s." % name)
            else:
                with self.print_lock:
                    self.console.print(f"[bold yellow]⚠ [Client][/bold yellow] Not subscribed to [bold cyan]{name}[/bold cyan].")
            return

        call, _ = entry
        call.cancel()
        self.sub_stats.pop(name, None)

        if not self.interactive:
            print("[Client] Unsubscribed from %s." % name)
        else:
            with self.print_lock:
                self.console.print(f"[bold yellow]🔌 [Client][/bold yellow] Unsubscribed from [bold cyan]'{name}'[/bold cyan].")

    # ---------------------------------------------------- Async Notifications --

    def on_update(self, update):
        # Monotonicity check: drop stale or out-of-order updates
        cached = self.docs_cache.get(update.name)
        if cached and cached.get("version", 0) >= update.version:
            return
        self.known_docs.add(update.name)
        self.docs_cache[update.name] = {"version": update.version, "content": update.content}
        self.stats["updates"] += 1

        if update.name in self.sub_stats:
            self.sub_stats[update.name]["updates"] += 1
            self.sub_stats[update.name]["last_v"] = update.version

        if not self.interactive:
            with self.print_lock:
                print("[Update] Document %s modified (v%d)." % (update.name, update.version))
                print(update.content)
                sys.stdout.flush()
            return

        with self.print_lock:
            # Clear current input line cleanly so the prompt isn't corrupted
            sys.stdout.write("\r\033[K")
            sys.stdout.flush()

            if update.edited_by == self.client_id:
                author_tag = "[green]You (self)[/green]"
            elif update.edited_by > 0:
                author_tag = f"[bold magenta]Peer Client #{update.edited_by}[/bold magenta]"
            elif update.version == 1:
                author_tag = "[dim]Initial Snapshot[/dim]"
            else:
                author_tag = "[dim]Server[/dim]"

            header = f"[bold magenta]⚡ LIVE STREAM UPDATE[/bold magenta] • [bold cyan]{update.name}[/bold cyan] ➔ [bold yellow]v{update.version}[/bold yellow]"
            meta = f"Editor: {author_tag} • {len(update.content)} chars"

            content_text = Text(update.content) if update.content else Text("[Empty Document]", style="dim italic")

            if self.term_width < 75:
                title = f"⚡ [bold magenta]Live Update[/bold magenta]: [bold cyan]{update.name}[/bold cyan] [bold yellow]v{update.version}[/bold yellow]"
                sub = f"{author_tag} • {len(update.content)} chars"
                panel = Panel(
                    content_text,
                    title=title,
                    subtitle=f"[dim]{sub}[/dim]",
                    subtitle_align="right",
                    border_style="magenta",
                    box=ROUNDED,
                    padding=(0, 1),
                )
            else:
                header = f"[bold magenta]⚡ LIVE STREAM UPDATE[/bold magenta] • [bold cyan]{update.name}[/bold cyan] ➔ [bold yellow]v{update.version}[/bold yellow]"
                meta = f"Editor: {author_tag} • {len(update.content)} chars"
                panel = Panel(
                    content_text,
                    title=f"{header}  [dim]({meta})[/dim]",
                    title_align="left",
                    border_style="magenta",
                    box=ROUNDED,
                    padding=(0, 1),
                )
            self.console.print(panel)

            try:
                readline.redisplay()
            except Exception:
                pass

    def on_sub_error(self, name, error):
        msg = error_text(error)
        if not self.interactive:
            with self.print_lock:
                print("[Error] subscription to %s failed: %s" % (name, msg), flush=True)
            return

        with self.print_lock:
            sys.stdout.write("\r\033[K")
            sys.stdout.flush()
            self.console.print(f"[bold red]✗ [Error][/bold red] Subscription to [cyan]{name}[/cyan] failed: {msg}")
            try:
                readline.redisplay()
            except Exception:
                pass

    def print_error(self, message):
        if not self.interactive:
            print("[Error] %s" % message)
        else:
            with self.print_lock:
                self.console.print(f"[bold red]✗ [Error][/bold red] {message}")

    # ---------------------------------------------------- Interactive Helpers --

    def print_banner(self):
        if self.term_width < 75:
            grid = Table.grid(expand=True)
            grid.add_column(justify="left")
            grid.add_row(Text.from_markup("⚡ [bold cyan]DOCS-gRPC COLLABORATIVE EDITOR[/bold cyan]"))
            grid.add_row(Text.from_markup(f"🟢 [bold green]ONLINE[/bold green] [dim](v1.0)[/dim] • ID: [yellow]#{self.client_id}[/yellow]"))
            grid.add_row(Text.from_markup(f"[dim]Endpoint: [cyan]{self.address}[/cyan][/dim]"))
            grid.add_row(Text.from_markup("[dim]Type [cyan]help[/cyan] for guide • [cyan]<TAB>[/cyan] autocompletes[/dim]"))

            panel = Panel(
                grid,
                title="[bold white]Distributed Systems • S3Q1[/bold white]",
                border_style="cyan",
                box=ROUNDED,
                padding=(0, 1),
            )
            self.console.print(panel)
        else:
            grid = Table.grid(expand=True, padding=(0, 1))
            grid.add_column(justify="left", ratio=1)
            grid.add_column(justify="right", ratio=1)
            grid.add_row(
                Text.from_markup("⚡ [bold cyan]DOCS-gRPC COLLABORATIVE EDITOR[/bold cyan]"),
                Text.from_markup("🟢 [bold green]ONLINE[/bold green] [dim](v1.0.0)[/dim]")
            )
            grid.add_row(
                Text.from_markup(f"[dim white]Endpoint: [cyan]{self.address}[/cyan][/dim white]"),
                Text.from_markup(f"[dim white]Client ID: [yellow]#{self.client_id}[/yellow][/dim white]")
            )
            panel = Panel(
                grid,
                title="[bold white]Distributed Systems • Section 3 Q1[/bold white]",
                subtitle="[dim]Type [bold cyan]help[/bold cyan] for guide • [bold cyan]<TAB>[/bold cyan] autocompletes • [bold cyan]exit[/bold cyan] to leave[/dim]",
                subtitle_align="right",
                border_style="cyan",
                box=ROUNDED,
                padding=(0, 2),
            )
            self.console.print(panel)

    def print_help(self):
        if not self.interactive:
            print(LEGACY_HELP)
            return

        if self.term_width < 75:
            table = Table(
                title="Interactive Command Reference",
                box=ROUNDED,
                header_style="bold cyan",
                border_style="blue",
                expand=True
            )
            table.add_column("Command", style="bold cyan", width=12)
            table.add_column("Usage & Description", style="white")

            table.add_row("create", "create <doc> [\"text\"]\n[dim]Create new document[/dim]")
            table.add_row("open", "open <doc>\n[dim]Fetch latest text[/dim]")
            table.add_row("edit", "edit <doc> <pos> \"text\"\n[dim]Atomic insert at pos[/dim]")
            table.add_row("subscribe", "subscribe <doc>\n[dim]Stream live updates[/dim]")
            table.add_row("unsubscribe", "unsubscribe <doc>\n[dim]Stop streaming[/dim]")
            table.add_row("status", "status\n[dim]Telemetry & streams[/dim]")
            table.add_row("list", "list [dim](show all docs)[/dim]")
            table.add_row("clear", "clear [dim](clear screen)[/dim]")
            table.add_row("help", "help [dim](this guide)[/dim]")
            table.add_row("exit", "exit [dim](quit client)[/dim]")

            with self.print_lock:
                self.console.print(table)
            return

        table = Table(
            title="Interactive Command Reference",
            box=ROUNDED,
            header_style="bold cyan",
            border_style="blue",
        )
        table.add_column("Command", style="bold cyan")
        table.add_column("Arguments", style="yellow")
        table.add_column("Description", style="white")
        table.add_column("Example", style="dim green")

        # Section 1: Documents
        table.add_row(
            "create", "<name> [\"<content>\"]",
            "Create a new named document with optional initial text",
            'create report.txt "Initial body"'
        )
        table.add_row(
            "open", "<name>",
            "Fetch and display the latest version and content",
            'open report.txt'
        )
        table.add_row(
            "edit", "<name> <pos> \"<text>\"",
            "Atomically insert text at character index (server-serialized)",
            'edit report.txt 6 "Distributed "'
        )

        # Section 2: Real-time streams
        table.add_row(
            "subscribe", "<name>",
            "Attach background stream to receive live peer updates",
            'subscribe report.txt'
        )
        table.add_row(
            "unsubscribe", "<name>",
            "Stop receiving background updates for a document",
            'unsubscribe report.txt'
        )

        # Section 3: Navigation & Session
        table.add_row(
            "status", "",
            "Show gRPC connection details, subscriptions, and metrics",
            'status'
        )
        table.add_row(
            "list", "",
            "List all documents touched or cached during this session",
            'list'
        )
        table.add_row(
            "clear", "",
            "Clear screen and redisplay header banner",
            'clear'
        )
        table.add_row(
            "help", "",
            "Display this interactive guide",
            'help'
        )
        table.add_row(
            "exit", "",
            "Close streams and disconnect gracefully",
            'exit'
        )

        with self.print_lock:
            self.console.print(table)

    def print_status(self):
        with self.print_lock:
            sub_count = len(self.subscriptions)
            active_info = f"[bold cyan]{self.active_doc}[/bold cyan]" if self.active_doc else "[dim]None[/dim]"

            if self.term_width < 75:
                info_table = Table.grid(expand=True, padding=(0, 1))
                info_table.add_column(style="bold white", width=14)
                info_table.add_column(style="cyan")

                info_table.add_row("Endpoint:", self.address)
                info_table.add_row("Client ID:", f"#{self.client_id}")
                info_table.add_row("Active Doc:", active_info)
                info_table.add_row("Streams:", f"{sub_count} active")
                info_table.add_row("Commands:", f"{self.stats['commands']} (edits: {self.stats['edits']})")
                info_table.add_row("Updates Recv:", str(self.stats["updates"]))
                info_table.add_row("Status:", "[green]CONNECTED[/green]")

                panel = Panel(
                    info_table,
                    title="🔍 [bold white]Session Status[/bold white]",
                    border_style="cyan",
                    box=ROUNDED,
                    padding=(0, 1)
                )
                self.console.print(panel)

                if self.subscriptions:
                    table = Table(
                        title="Active Subscriptions",
                        box=ROUNDED,
                        header_style="bold magenta",
                        border_style="magenta",
                        expand=True
                    )
                    table.add_column("Document", style="cyan")
                    table.add_column("Recv", style="yellow", justify="right", width=8)
                    table.add_column("Ver", style="white", justify="right", width=6)

                    for name, stats in self.sub_stats.items():
                        table.add_row(
                            name,
                            str(stats.get("updates", 0)),
                            f"v{stats.get('last_v', '?')}"
                        )
                    self.console.print(table)
            else:
                info_table = Table.grid(expand=True, padding=(0, 2))
                info_table.add_column(style="bold white", width=18)
                info_table.add_column(style="cyan")
                info_table.add_column(style="bold white", width=18)
                info_table.add_column(style="cyan")

                info_table.add_row("Server Endpoint:", self.address, "Client ID:", f"#{self.client_id}")
                info_table.add_row("Active Document:", active_info, "Active Streams:", f"{sub_count} subscribed")
                info_table.add_row("Commands Run:", str(self.stats["commands"]), "Edits Submitted:", str(self.stats["edits"]))
                info_table.add_row("Updates Recv:", str(self.stats["updates"]), "Channel Status:", "[green]CONNECTED[/green]")

                panel = Panel(
                    info_table,
                    title="🔍 [bold white]Session Status & Telemetry[/bold white]",
                    border_style="cyan",
                    box=ROUNDED,
                    padding=(0, 1)
                )
                self.console.print(panel)

                if self.subscriptions:
                    table = Table(
                        title="Active Real-Time Subscriptions",
                        box=ROUNDED,
                        header_style="bold magenta",
                        border_style="magenta",
                        expand=True
                    )
                    table.add_column("Document", style="cyan", ratio=2)
                    table.add_column("Stream Status", style="green", width=15)
                    table.add_column("Updates Recv", style="yellow", justify="right", width=14)
                    table.add_column("Latest Version", style="white", justify="right", width=14)

                    for name, stats in self.sub_stats.items():
                        table.add_row(
                            name,
                            "● STREAMING",
                            str(stats.get("updates", 0)),
                            f"v{stats.get('last_v', '?')}"
                        )
                    self.console.print(table)

    def print_list(self):
        with self.print_lock:
            if not self.known_docs:
                self.console.print("[dim]No documents tracked yet. Use 'create' or 'open' to begin.[/dim]")
                return

            if self.term_width < 75:
                table = Table(
                    title="Tracked Documents",
                    box=ROUNDED,
                    header_style="bold cyan",
                    border_style="blue",
                    expand=True
                )
                table.add_column("Document", style="bold cyan")
                table.add_column("Ver (chars)", style="yellow", justify="right", width=13)
                table.add_column("Stream", style="magenta", justify="center", width=9)

                for name in sorted(self.known_docs):
                    cached = self.docs_cache.get(name, {})
                    v_str = f"v{cached.get('version', '?')}"
                    l_str = f"{len(cached.get('content', ''))}c" if "content" in cached else "?c"
                    sub_str = "[bold green]LIVE[/bold green]" if name in self.subscriptions else "[dim]off[/dim]"
                    doc_label = f"{name} ⭐" if name == self.active_doc else name
                    table.add_row(doc_label, f"{v_str} ({l_str})", sub_str)

                self.console.print(table)
                return

            table = Table(
                title="Tracked Documents in Session",
                box=ROUNDED,
                header_style="bold cyan",
                border_style="blue",
                expand=True
            )
            table.add_column("Document Name", style="bold cyan", ratio=2)
            table.add_column("Cached Version", style="yellow", justify="right", width=16)
            table.add_column("Length (chars)", style="white", justify="right", width=16)
            table.add_column("Subscription", style="magenta", justify="center", width=16)
            table.add_column("Active Context", justify="center", width=16)

            for name in sorted(self.known_docs):
                cached = self.docs_cache.get(name, {})
                v_str = f"v{cached.get('version', '?')}"
                l_str = str(len(cached.get("content", ""))) if "content" in cached else "?"
                sub_str = "[bold green]YES[/bold green]" if name in self.subscriptions else "[dim]no[/dim]"
                active_str = "⭐ [bold yellow]SELECTED[/bold yellow]" if name == self.active_doc else ""
                table.add_row(name, v_str, l_str, sub_str, active_str)

            self.console.print(table)

    def get_prompt(self):
        c_cyan = "\001\033[1;36m\002"
        c_green = "\001\033[1;32m\002"
        c_yellow = "\001\033[1;33m\002"
        c_dim = "\001\033[2m\002"
        c_reset = "\001\033[0m\002"

        cli_str = f"{c_cyan}doc-cli{c_reset}"

        if self.term_width < 70:
            if self.active_doc and self.active_doc in self.docs_cache:
                v = self.docs_cache[self.active_doc].get("version", "?")
                return f"{cli_str} {c_yellow}[{self.active_doc}:v{v}]{c_reset} {c_green}❯{c_reset} "
            elif self.active_doc:
                return f"{cli_str} {c_yellow}[{self.active_doc}]{c_reset} {c_green}❯{c_reset} "
            else:
                return f"{cli_str} {c_dim}#{self.client_id}{c_reset} {c_green}❯{c_reset} "

        id_str = f"{c_dim}#{self.client_id}{c_reset}"

        if self.active_doc and self.active_doc in self.docs_cache:
            v = self.docs_cache[self.active_doc].get("version", "?")
            doc_str = f" {c_yellow}[{self.active_doc}:v{v}]{c_reset}"
        elif self.active_doc:
            doc_str = f" {c_yellow}[{self.active_doc}]{c_reset}"
        else:
            doc_str = ""

        return f"{cli_str} {id_str}{doc_str} {c_green}❯{c_reset} "

    def complete(self, text, state):
        """Readline tab auto-completer for commands and document names."""
        try:
            buf = readline.get_line_buffer()
            parts = buf.split()

            if len(parts) == 0 or (len(parts) == 1 and not buf.endswith(" ")):
                options = [c for c in self.COMMANDS if c.startswith(text.lower())]
            elif len(parts) >= 1:
                cmd = parts[0].lower()
                if cmd in ("open", "edit", "subscribe", "unsubscribe"):
                    if len(parts) == 1 or (len(parts) == 2 and not buf.endswith(" ")):
                        options = [d for d in sorted(self.known_docs) if d.startswith(text)]
                    else:
                        options = []
                else:
                    options = []
            else:
                options = []

            if state < len(options):
                return options[state]
            return None
        except Exception:
            return None

    def close(self):
        for call, _ in list(self.subscriptions.values()):
            try:
                call.cancel()
            except Exception:
                pass
        self.channel.close()


# ------------------------------------------------------------- Main / CLI --

def parse_args():
    address = "localhost:50051"
    interactive = None

    for arg in sys.argv[1:]:
        if arg in ("-i", "--interactive", "--color"):
            interactive = True
        elif arg in ("-p", "--plain", "--raw", "--no-color"):
            interactive = False
        elif arg in ("-h", "--help"):
            print(__doc__)
            sys.exit(0)
        elif not arg.startswith("-"):
            address = arg

    if interactive is None:
        interactive = sys.stdin.isatty() and sys.stdout.isatty()

    return address, interactive


def setup_history():
    histfile = os.path.expanduser("~/.doc_cli_history")
    try:
        if os.path.exists(histfile):
            readline.read_history_file(histfile)
        readline.set_history_length(1000)
        atexit.register(lambda: readline.write_history_file(histfile))
    except Exception:
        pass


def execute_line(client, line):
    line = line.strip()
    if not line:
        return True

    try:
        parts = shlex.split(line)
    except ValueError as parse_error:
        client.print_error(str(parse_error))
        return True

    command = parts[0].lower()
    try:
        if command in ("exit", "quit", "5", "q"):
            return False
        elif command in ("help", "?"):
            client.print_help()
        elif command in ("status", "info"):
            client.print_status()
        elif command in ("list", "ls"):
            client.print_list()
        elif command in ("clear", "cls"):
            if client.interactive:
                os.system("clear")
                client.print_banner()
            else:
                print()
        elif command in ("create", "1") and len(parts) >= 2:
            content = parts[2] if len(parts) > 2 else ""
            client.create(parts[1], content)
        elif command in ("open", "2") and len(parts) == 2:
            client.open(parts[1])
        elif command in ("open", "2") and len(parts) == 1 and client.active_doc:
            client.open(client.active_doc)
        elif command in ("edit", "3") and len(parts) >= 4:
            position = int(parts[2])
            text = parts[3]
            client.edit(parts[1], position, text)
        elif command in ("edit", "3") and len(parts) == 3 and client.active_doc and parts[1].isdigit():
            # Shorthand: edit <position> "<text>" on active document
            position = int(parts[1])
            text = parts[2]
            client.edit(client.active_doc, position, text)
        elif command in ("subscribe", "4") and len(parts) == 2:
            client.subscribe(parts[1])
        elif command in ("subscribe", "4") and len(parts) == 1 and client.active_doc:
            client.subscribe(client.active_doc)
        elif command == "unsubscribe" and len(parts) == 2:
            client.unsubscribe(parts[1])
        elif command == "unsubscribe" and len(parts) == 1 and client.active_doc:
            client.unsubscribe(client.active_doc)
        else:
            client.print_error("unknown command: %s" % line)
    except grpc.RpcError as rpc_error:
        client.print_error(error_text(rpc_error))
    except ValueError:
        client.print_error("position must be an integer")

    return True


def main():
    address, interactive = parse_args()
    client = DocumentClient(address, interactive=interactive)

    if not interactive:
        print("connected to %s (client id %d)" % (address, client.client_id),
              file=sys.stderr)
        print(LEGACY_HELP, file=sys.stderr)
        try:
            for line in sys.stdin:
                if not execute_line(client, line):
                    break
        finally:
            client.close()
    else:
        # Interactive TUI mode with optional readline
        if readline:
            setup_history()
            try:
                readline.set_completer_delims(" \t\n")
                readline.set_completer(client.complete)
                readline.parse_and_bind("tab: complete")
            except Exception:
                pass

        client.print_banner()
        try:
            while True:
                try:
                    prompt = client.get_prompt()
                    line = input(prompt)
                except EOFError:
                    print()
                    break
                except KeyboardInterrupt:
                    print()
                    continue

                if not execute_line(client, line):
                    break
        finally:
            client.close()
            with client.print_lock:
                client.console.print("[dim]Disconnected from server. Goodbye![/dim]")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
