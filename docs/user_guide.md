# Memento Node

Memento is the Sovereign Memory Agent / Intelligence Index for the Vilaros Ecosystem.

## User Guide
As a Vilaros user, Memento is the component that strictly remembers who you are, what files you have seen, your knowledge items, and every action that has occurred natively on your device.

When you drop a file into your watched folders, Memento uses Hera to automatically read it, semantically index it, and inject it into your private local `qdrant` vector database. 
You do not interact with Memento via the Web. It is a completely headless indexing backend. Vilaros OS components or the native agent (Imaginclaw) talks directly to Memento using instant Unix sockets (`/tmp/memento.sock`).

## Securing your Knowledge
Because Memento runs purely as an IPC daemon, it is physically impossible for an external network actor to query your memory vectors. The port is bound ONLY to `/tmp/memento.sock`, requiring local unix permissions to read.

### Troubleshooting
If the intelligence index hangs or stops adding new files:
1. Reload via `pm2 restart memento`.
2. Ensure the daemon has R/W permissions on your watched folders.
3. Check the internal IPC loop via the Vilaros OS Audit Dashboard.
