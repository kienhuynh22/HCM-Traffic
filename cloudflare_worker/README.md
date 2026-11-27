# Cloudflare Worker Scheduler Guard for HCM-Traffic

Independent cron monitor deployed on Cloudflare Workers to guarantee that the `afternoon_37slots` capture session launches early enough for the 16:00 Vietnam milestone every day (triggering at **15:35** and **15:45** Vietnam time / `08:35` & `08:45` UTC), while preventing any duplicate runs.

---

## 1. How It Works

1. **Trigger Times (`wrangler.toml`):**
   - Triggers daily at `08:35 UTC` (**15:35 ICT**) and `08:45 UTC` (**15:45 ICT**), well inside the `[15:30, 16:00)` startup window before slot 1 at `16:00`.
   - At `15:35`, if no production live run is active or already captured today, it dispatches `capture_13paths.yml`.
   - At `15:45`, it checks again; if the `15:35` dispatch or GitHub cron (`15:40`) is already queued/running, it skips dispatch. If `15:35` failed or was skipped, `15:45` acts as a second safety net.
2. **Duplicate & Partial-Capture Prevention Check:**
   - Queries GitHub Actions API for `capture_13paths.yml` workflow runs.
   - **Active Run:** If a production live run for today is `queued`, `in_progress`, `waiting`, `pending`, or `requested` (including manual runs queued behind concurrency), it **skips dispatch**. Active `dry-run` or `trial_3slots` runs do not suppress production dispatch.
   - **Completed or Partial Capture (including `conclusion == 'failure'` like Run #20):** Inspects recent completed runs regardless of conclusion. If a run captured production slots for today (`afternoon_37slots` bundle >= 2 MB or capture step ran past 16:00 ICT for >= 120s), it **skips dispatch** so partial sessions (e.g., 36/37 slots) are never re-captured in full.
   - **Distinction:** Ignores `dry-run`, `trial_3slots`, early-halted runs, and 0-slot midnight timeout runs (e.g. Run #10, #21).
3. **Dispatch (`POST /dispatches`):**
   - If and only if no active or captured production live run exists for today, it sends `POST /dispatches` on `master` with explicit string inputs matching `capture_13paths.yml`:
     - `mode`: `"afternoon_37slots"`
     - `dry_run`: `"false"`
     - `allow_init_state`: `"false"`
     - `date`: `"<YYYY-MM-DD>"` (Vietnam time)
     - `automated`: `"true"`
     - `import_external_source`: `""`
     - `import_traffic`: `"0"`
     - `import_weather`: `"0"`
4. **Fail-Safe Invariant:**
   - If the GitHub API cannot be reached or returns a non-OK HTTP status, the Worker **throws an error and marks the execution failed**. It never executes a blind dispatch when repository status is unknown.

---

## 2. Minimal Required Permissions (Least-Privilege)

Create a **Fine-grained Personal Access Token (PAT)** in GitHub:
1. Go to **GitHub Settings** -> **Developer Settings** -> **Personal Access Tokens** -> **Fine-grained tokens** -> **Generate new token**.
2. **Token Name:** `hcm-traffic-cf-worker-dispatch`
3. **Expiration:** 90 days or 1 year (set reminder to rotate).
4. **Resource Owner:** `kienhuynh22`
5. **Repository Access:** **Only select repositories** -> Select `kienhuynh22/HCM-Traffic`.
6. **Repository Permissions:**
   - **Actions:** `Read and write`
     - *Read* is required to query workflow runs, jobs, and artifacts.
     - *Write* is required to trigger workflow dispatches (`POST /dispatches`).
   - **Metadata:** `Read-only` (automatically included by GitHub).
   - **All other permissions:** `No access`.

---

## 3. Secret Configuration in Cloudflare

The Worker requires one secret: `GITHUB_TOKEN`.

### Option A: Via Cloudflare Dashboard
1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/).
2. Navigate to **Workers & Pages** -> Select `hcm-traffic-scheduler-guard`.
3. Go to **Settings** -> **Variables and Secrets**.
4. Under **Secrets**, click **Add**:
   - **Variable name:** `GITHUB_TOKEN`
   - **Value:** Paste the fine-grained PAT.
   - Click **Save and deploy**.

### Option B: Via Wrangler CLI
```bash
cd cloudflare_worker
npx wrangler secret put GITHUB_TOKEN
```

---

## 4. Local Testing and Deployment

```bash
cd cloudflare_worker
npm test
npm install
echo "GITHUB_TOKEN=your_pat_token_here" > .dev.vars
npm run dev
# Test endpoints:
# GET http://localhost:8787/status
# GET http://localhost:8787/check
```

Deploy **only after merging PR #5 into `master`** (so `capture_13paths.yml` on `master` accepts the `automated` input):
```bash
npm run deploy
```
