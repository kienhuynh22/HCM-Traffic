/**
 * Cloudflare Worker Cron Guard for HCM-Traffic Capture.
 * Scheduled to trigger at 15:45 Vietnam time (08:45 UTC).
 * 
 * Logic:
 * 1. Checks current date in Asia/Ho_Chi_Minh.
 * 2. Queries GitHub Actions API for capture_13paths.yml runs.
 * 3. If a production live session is queued, in-progress, or already completed today: SKIPS dispatch.
 * 4. If no production live run exists: DISPATCHES workflow on master.
 * 5. If GitHub API is unreachable or returns error: THROWS error (fails safely, never blind dispatch).
 */

function getVietnamDateStr(date = new Date()) {
  // Format YYYY-MM-DD in Asia/Ho_Chi_Minh
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Ho_Chi_Minh",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(date); // returns YYYY-MM-DD
}

function getVietnamTimeStr(date = new Date()) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Ho_Chi_Minh",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

async function checkAndDispatch(env) {
  const repo = env.GITHUB_REPO || "kienhuynh22/HCM-Traffic";
  const workflowFile = env.WORKFLOW_FILE || "capture_13paths.yml";
  const branch = env.TARGET_BRANCH || "master";
  const token = env.GITHUB_TOKEN;

  if (!token) {
    throw new Error("Missing required secret: GITHUB_TOKEN in Worker environment");
  }

  const todayVN = getVietnamDateStr();
  const timeVN = getVietnamTimeStr();
  console.log(`[GUARD] Running check at ${timeVN} ICT for date ${todayVN} on ${repo}`);

  const headers = {
    "Authorization": `Bearer ${token}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "HCM-Traffic-Cloudflare-Worker-Guard",
    "X-GitHub-Api-Version": "2022-11-28",
  };

  // 1. Query recent workflow runs
  const runsUrl = `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/runs?per_page=15`;
  const runsRes = await fetch(runsUrl, { headers });

  if (!runsRes.ok) {
    const errorBody = await runsRes.text();
    throw new Error(`GitHub API query failed (HTTP ${runsRes.status}): ${errorBody}`);
  }

  const runsData = await runsRes.json();
  const runs = runsData.workflow_runs || [];

  // Check 1: Any run currently queued, in-progress, or waiting?
  for (const r of runs) {
    const status = r.status; // 'queued', 'in_progress', 'waiting', 'completed'
    if (["queued", "in_progress", "waiting"].includes(status)) {
      const msg = `Active run already in progress: Run ID ${r.id} (status: ${status}, created: ${r.created_at}). Skipping dispatch.`;
      console.log(`[GUARD] ${msg}`);
      return { action: "skipped", reason: "ACTIVE_RUN_EXISTS", run_id: r.id, status };
    }
  }

  // Check 2: Any run completed with successful live production capture for today?
  for (const r of runs) {
    if (r.conclusion === "success") {
      const runDateVN = getVietnamDateStr(new Date(r.created_at));
      if (runDateVN === todayVN) {
        // Query artifacts of this successful run to verify it was a live production capture
        const artUrl = `https://api.github.com/repos/${repo}/actions/runs/${r.id}/artifacts`;
        const artRes = await fetch(artUrl, { headers });
        if (artRes.ok) {
          const artData = await artRes.json();
          const artifacts = artData.artifacts || [];
          const prodArtifact = artifacts.find(a => 
            a.name.startsWith(`capture-outputs-afternoon_37slots-${todayVN}`) && a.size_in_bytes > 1_000_000
          );
          if (prodArtifact) {
            const msg = `Production live session already completed today for ${todayVN}: Run ID ${r.id} (artifact: ${prodArtifact.name}). Skipping dispatch.`;
            console.log(`[GUARD] ${msg}`);
            return { action: "skipped", reason: "ALREADY_COMPLETED", run_id: r.id };
          }
        }
      }
    }
  }

  // 2. No active or completed live run found: dispatch fallback run
  console.log(`[GUARD] No active or completed production run found for ${todayVN}. Dispatching ${workflowFile}...`);

  const dispatchUrl = `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/dispatches`;
  const dispatchPayload = {
    ref: branch,
    inputs: {
      mode: "afternoon_37slots",
      dry_run: false,
      allow_init_state: false,
      date: todayVN,
      automated: true,
      import_external_source: "",
      import_date: todayVN,
      import_traffic: 0,
      import_weather: 0,
    },
  };

  const dispatchRes = await fetch(dispatchUrl, {
    method: "POST",
    headers,
    body: JSON.stringify(dispatchPayload),
  });

  if (dispatchRes.status !== 204) {
    const dispatchErr = await dispatchRes.text();
    throw new Error(`GitHub API dispatch failed (HTTP ${dispatchRes.status}): ${dispatchErr}`);
  }

  const successMsg = `Successfully dispatched ${workflowFile} for date ${todayVN} on branch ${branch}.`;
  console.log(`[GUARD] ${successMsg}`);
  return { action: "dispatched", date: todayVN, branch };
}

export default {
  // Cron handler triggered at 08:45 UTC (15:45 VN)
  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      checkAndDispatch(env).catch(err => {
        console.error(`[GUARD ERROR] ${err.message}`);
      })
    );
  },

  // HTTP handler for manual testing, monitoring, or webhook
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/status") {
      const todayVN = getVietnamDateStr();
      const timeVN = getVietnamTimeStr();
      return new Response(JSON.stringify({
        status: "ok",
        timezone: "Asia/Ho_Chi_Minh",
        current_time_ict: timeVN,
        target_date_ict: todayVN,
        repo: env.GITHUB_REPO,
        workflow: env.WORKFLOW_FILE,
        cron_schedule: "45 8 * * * (15:45 Asia/Ho_Chi_Minh)",
      }, null, 2), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (url.pathname === "/check" || url.pathname === "/trigger") {
      try {
        const result = await checkAndDispatch(env);
        return new Response(JSON.stringify({ success: true, result }, null, 2), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ success: false, error: err.message }, null, 2), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response(
      "HCM-Traffic Cloudflare Worker Scheduler Guard. Use /status or /check.",
      { headers: { "Content-Type": "text/plain" } }
    );
  },
};
