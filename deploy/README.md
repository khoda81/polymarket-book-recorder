# Deployment

The recorder and the [frontend](https://github.com/khoda81/polymarket-book-vis)
are deployed to one VPS. Caddy serves the frontend, proxies `/api/recorder/*`
to the recorder on loopback, and puts everything behind basic auth.

```
browser ─HTTPS─> Caddy :443 (basic auth)
                  ├─ /api/recorder/*  → 127.0.0.1:3001  polyviz-recorder.service
                  └─ /*               → /opt/polyviz/frontend/current
```

Every push to `main` in either repository runs its `Release` workflow. The
workflow builds the commit, publishes it as a GitHub release tagged
`vYYYY.MM.DD-<short sha>` with a tarball and its `.sha256`, and then SSHes to
the VPS. The deploy key can only run
`polyviz-deploy <component> <tag>`, which downloads and verifies the release
asset, unpacks it under `/opt/polyviz/<component>/releases/<tag>`, and
atomically repoints `current`. A recorder deploy restarts the service and rolls
back to the previous release if `/api/recorder/health` does not answer within
two minutes. The newest five releases are kept.

## One-time setup

1. Create a Debian 12+ or Ubuntu 24.04+ arm64 VPS; recorder releases are
   built only for aarch64. Point a DNS A record at it. If the DNS is on
   Cloudflare, leave the record unproxied, because CI reaches the host over
   SSH by that name.
2. Generate the deploy key locally:

       ssh-keygen -t ed25519 -N '' -C polyviz-deploy -f polyviz-deploy

3. On the VPS, from a checkout of this repository:

       sudo deploy/bootstrap.sh --domain polyviz.example.com --login me \
         --deploy-key "$(cat polyviz-deploy.pub)"

4. In **both** repositories, go to Settings → Environments → `production` and add:
   - variable `DEPLOY_HOST`: the domain
   - variable `DEPLOY_KNOWN_HOSTS`: the output of `ssh-keyscan -t ed25519 <domain>`
   - secret `DEPLOY_SSH_KEY`: the contents of `polyviz-deploy` (the private key)

   Until `DEPLOY_HOST` is set, releases are published without deploying.

5. Optional: bring over existing history before the first recorder deploy:

       # locally, with the recorder stopped
       sqlite3 .data/rust-recorder.sqlite 'PRAGMA wal_checkpoint(TRUNCATE)'
       scp .data/rust-recorder.sqlite vps:
       # on the VPS
       sudo install -o polyviz -g polyviz -m 640 rust-recorder.sqlite /var/lib/polyviz/recorder.sqlite

## Releasing

Merge to `main`. The two repositories deploy independently; the protobuf
schema keeps an older frontend and a newer recorder (or the reverse)
compatible, so they need not be released together.

## Operating

    journalctl -u polyviz-recorder -f                           # recorder logs
    sudo -u polyviz-deploy polyviz-deploy recorder <older tag>  # roll back by hand
    curl -s 127.0.0.1:3001/api/recorder/health                  # on the VPS

You can also re-run the `deploy` job of an older `Release` run in GitHub
Actions to roll back.
