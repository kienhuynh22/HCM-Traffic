# Cloudflare Worker Scheduler Guard for HCM-Traffic

Independent cron monitor deployed on Cloudflare Workers to guarantee that the `afternoon_37slots` capture session launches on time every day (~15:45 Vietnam time / 08:45 UTC), while preventing any duplicate runs.

---

## 1. How It Works

1. **Trigger Time:**
   - Triggers every day at `08:45 UTC` (`15:45 Asia/Ho_Chi_Minh`), exactly 15 minutes before the first afternoon slot (16:00).
   - Serves as an independent fallback in case GitHub Actions native cron (`08:40 UTC` / `15:40 ICT`) experiences scheduling delay, server jitter, or drops.
2. **Duplicate Prevention Check:**
   - Queries GitHub Actions API for `capture_13paths.yml` workflow runs.
   - **Active Run:** If any run is currently `queued`, `in_progress`, or `waiting`, it **skips dispatch**.
   - **Completed Run:** If a production live session has already succeeded for today (`date` in Vietnam time with full artifact), it **skips dispatch**.
   - **Dry-run & Trial Distinction:** Ignores dry-runs or trials (does not treat them as production capture).
3. **Dispatch:**
   - If and only if NO production run exists or is running for today, it sends a `POST /dispatches` request to GitHub Actions with:
     - `ref`: `master`
     - `mode`: `afternoon_37slots`
     - `dry_run`: `false`
     - `allow_init_state`: `false`
     - `date`: `<current_date_in_vietnam>` (YYYY-MM-DD)
     - `automated`: `true`
     - `import_external_source`: `""`
4. **Fail-Safe Invariant:**
   - If the GitHub API cannot be reached or returns an HTTP error, the Worker **throws an error and halts**. It never executes a blind dispatch when repository status is unknown.

---

## 2. Minimal Required Permissions (Least-Privilege)

To allow the Worker to inspect runs and dispatch the workflow without giving access to sensitive repository data or administrative settings:

Create a **Fine-grained Personal Access Token (PAT)** in GitHub:
1. Go to **GitHub Settings** -> **Developer Settings** -> **Personal Access Tokens** -> **Fine-grained tokens** -> **Generate new token**.
2. **Token Name:** `hcm-traffic-cf-worker-dispatch`
3. **Expiration:** 90 days or 1 year (set reminder to rotate).
4. **Resource Owner:** `kienhuynh22`
5. **Repository Access:** **Only select repositories** -> Select `kienhuynh22/HCM-Traffic`.
6. **Repository Permissions:**
   - **Actions:** `Read and write`
     - *Read* is required to query workflow runs and artifacts (`GET /repos/{owner}/{repo}/actions/...`).
     - *Write* is required to trigger workflow dispatches (`POST /repos/{owner}/{repo}/actions/workflows/.../dispatches`).
   - **Metadata:** `Read-only` (automatically included by GitHub).
   - **All other permissions:** `No access` (do **NOT** grant access to Secrets, Administration, Contents write, Pull Requests, or Issues).

---

## 3. Secret Configuration in Cloudflare

The Worker requires one secret: `GITHUB_TOKEN`.

### Option A: Via Cloudflare Dashboard (Recommended)
1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/).
2. Navigate to **Workers & Pages** -> Click on `hcm-traffic-scheduler-guard` (after deploying).
3. Go to **Settings** -> **Variables and Secrets**.
4. Under **Secrets**, click **Add**:
   - **Variable name:** `GITHUB_TOKEN`
   - **Value:** Paste the fine-grained PAT generated in step 2.
   - Click **Save and deploy**.

### Option B: Via Wrangler CLI
Run the following command from the `cloudflare_worker` directory:
```bash
npx wrangler secret put GITHUB_TOKEN
# Paste your GitHub fine-grained PAT when prompted
```

---

## 4. Local Testing and Deployment

### Test Locally
```bash
cd cloudflare_worker
npm install
# Set temporary secret in .dev.vars for local testing:
echo "GITHUB_TOKEN=your_pat_token_here" > .dev.vars
npm run dev
```
In your browser or curl:
- `http://localhost:8787/status` -> Check current time in Vietnam and target date.
- `http://localhost:8787/trigger` -> Simulate check & dispatch logic.

### Deploy to Cloudflare
```bash
npm run deploy
```
Once deployed, Cloudflare will trigger the worker automatically according to `crons = ["45 8 * * *"]`.
