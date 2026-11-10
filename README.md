# bazaar-contrib-starlink

Connects a node to a Bazaar Compute server. The node takes the server's calls, runs them through its own sidecar, as `bcc` does, and reports its audit events. When the server cannot be reached, the node waits and tries again. The server's page for connecting a computer gives the command:

```sh
bcn contrib add bazaar-contrib-starlink \
    --enable starlink \
    --option server=https://bazaar.example.com \
    --option token=TOKEN
```

`token_env` names an environment variable holding the token instead; `token` wins when both are set.

To link a node to several servers, give its configuration a `starlink` entry for each.
