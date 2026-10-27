import argparse
import os
import signal
import subprocess
import sys
import time

from .client import Client, ClusterUnavailable


def local_peers(n, base_raft, base_http):
    return {i: ("127.0.0.1", base_raft + i, base_http + i) for i in range(1, n + 1)}


def peers_spec(peers):
    return ",".join(f"{i}={h}:{r}:{p}" for i, (h, r, p) in peers.items())


def cmd_serve(args):
    from .node import parse_peers, run
    run(args.id, parse_peers(args.peers), args.data)


def cmd_cluster(args):
    """Start a local cluster in child processes; ctrl-c stops it."""
    peers = local_peers(args.nodes, args.raft_port, args.http_port)
    spec = peers_spec(peers)
    procs = {}
    for i in peers:
        data = os.path.join(args.data, f"node{i}")
        procs[i] = subprocess.Popen([sys.executable, "-m", "quorum", "serve", "--id", str(i),
                                     "--peers", spec, "--data", data])
    print("cluster up:", " ".join(f"http://127.0.0.1:{p[2]}" for p in peers.values()), flush=True)
    reported = set()
    try:
        # a node dying is the whole point of a cluster: report it, keep the rest
        # running, and stop only on ctrl-c or when every node is gone
        while any(p.poll() is None for p in procs.values()):
            for i, p in procs.items():
                if p.poll() is not None and i not in reported:
                    reported.add(i)
                    print(f"node {i} exited with code {p.returncode}; the others keep going", flush=True)
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs.values():
            p.send_signal(signal.SIGTERM)
        for p in procs.values():
            p.wait()


def client_for(args):
    return Client(args.nodes.split(","))


def cmd_kv(args):
    c = client_for(args)
    try:
        if args.op == "get":
            v = c.get(args.key)
            print(v if v is not None else "(nil)")
        elif args.op == "put":
            c.put(args.key, args.value)
            print("OK")
        elif args.op == "delete":
            print("deleted" if c.delete(args.key) else "(no such key)")
        elif args.op == "cas":
            expected = None if args.expected == "-" else args.expected
            print("OK" if c.cas(args.key, expected, args.value) else "value didn't match")
    except ClusterUnavailable as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_status(args):
    c = client_for(args)
    for node in c.nodes:
        try:
            s = c.status(node)
            print(f"{node}  node {s['id']}  {s['role']:9} term {s['term']:<4} commit {s['commit']:<6} "
                  f"leader {s['leader']}")
        except OSError:
            print(f"{node}  down")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="quorum", description="a replicated key-value store on Raft")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run one node")
    s.add_argument("--id", type=int, required=True)
    s.add_argument("--peers", required=True, help="1=host:raft_port:http_port,2=...")
    s.add_argument("--data", required=True, help="directory for this node's log and state")
    s.set_defaults(fn=cmd_serve)

    c = sub.add_parser("cluster", help="start a local cluster")
    c.add_argument("--nodes", type=int, default=3)
    c.add_argument("--raft-port", type=int, default=9000)
    c.add_argument("--http-port", type=int, default=8000)
    c.add_argument("--data", default="data")
    c.set_defaults(fn=cmd_cluster)

    default_nodes = "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"
    for op in ("get", "put", "delete", "cas"):
        k = sub.add_parser(op)
        k.add_argument("key")
        if op == "cas":
            k.add_argument("expected", help="- for 'key doesn't exist'")
        if op in ("put", "cas"):
            k.add_argument("value")
        k.add_argument("--nodes", default=default_nodes)
        k.set_defaults(fn=cmd_kv, op=op)

    st = sub.add_parser("status")
    st.add_argument("--nodes", default=default_nodes)
    st.set_defaults(fn=cmd_status)

    args = p.parse_args(argv)
    return args.fn(args) or 0


if __name__ == "__main__":
    sys.exit(main())
