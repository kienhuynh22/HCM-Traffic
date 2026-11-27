/**
 * Cloudflare Worker Cron Guard for HCM-Traffic Capture.
 * Scheduled to trigger at 15:35 and 15:45 Vietnam time (08:35 and 08:45 UTC).
 *
 * Logic:
 * 1. Computes current date in Asia/Ho_Chi_Minh (YYYY-MM-DD).
 * 2. Queries GitHub Actions API for capture_13paths.yml runs.
 * 3. Checks for active (queued/in_progress/waiting) or completed/partial production live runs
 *    for today (including runs whose GitHub conclusion is 'failure' if capture ran, e.g. Run #20).
 *    Ignores dry-runs, trials, and midnight/early-stopped runs with 0 captured slots (e.g. #10, #21).
 * 4. If a production live run already exists/ran today: SKIPS dispatch.
 * 5. If no production live run exists today: DISPATCHES workflow on master with string inputs.
 * 6. If GitHub API is unreachable or returns an error: THROWS error (fails safely, never blind dispatch).
 */

export function getVietnamDateStr(date = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Ho_Chi_Minh",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(date);
}

export function getVietnamTimeStr(date = new Date()) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Ho_Chi_Minh",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

export function isActiveProductionLiveRun(run, todayVN) {
  if (!["queued", "in_progress", "waiting"].includes(run.status)) {
    return false;
  }
  const title = String(run.display_title || "");
  if (title.includes("[trial_3slots") || title.includes("dry-run")) {
    return false;
  }
  const explicitMatch = title.match(/\[afternoon_37slots live (\d{4}-\d{2}-\d{2})\]/);
  if (explicitMatch) {
    return explicitMatch[1] === todayVN;
  }
  if (run.event === "schedule" || title.includes("[afternoon_37slots live") || title === "Capture 13 Paths") {
    return getVietnamDateStr(new Date(run.created_at)) === todayVN;
  }
  return false;
}

export function classifyCompletedRunCapture(run, artifacts, jobs, todayVN) {
  const title = String(run.display_title || "");
  if (title.includes("[trial_3slots") || title.includes("dry-run")) {
    return null;
  }

  const prefix = `capture-outputs-afternoon_37slots-${todayVN}-`;
  const prodArtifact = (artifacts || []).find((a) => String(a.name || "").startsWith(prefix));
  if (!prodArtifact) {
    return null;
  }

  const slot1Utc = new Date(`${todayVN}T16:00:00+07:00`);
  for (const job of jobs || []) {
    for (const step of job.steps || []) {
      if (step.name !== "Execute Capture Run (Trial or Afternoon)") {
        continue;
      }
      const conc = step.conclusion;
      if (conc === "skipped") {
        return null;
      }
      const compDt = step.completed_at ? new Date(step.completed_at) : null;
      const startDt = step.started_at ? new Date(step.started_at) : null;
      const durS = compDt && startDt ? (compDt.getTime() - startDt.getTime()) / 1000 : 0;

      if (conc === "cancelled") {
        if (compDt && compDt < slot1Utc) {
          // Timed out sleeping before 16:00 ICT slot 1 (e.g. Run #10, #21) -> 0 captured slots
          return null;
        }
        if (compDt && compDt >= slot1Utc && prodArtifact.size_in_bytes >= 2_000_000) {
          return {
            reason: "PARTIAL_LIVE_CANCELLED",
            artifact: prodArtifact.name,
            run_conclusion: run.conclusion,
          };
        }
        return null;
      }

      if (conc === "success") {
        if (prodArtifact.size_in_bytes >= 2_000_000 || (compDt && compDt >= slot1Utc && durS >= 120)) {
          return {
            reason: run.conclusion === "success" ? "ALREADY_COMPLETED" : "PARTIAL_LIVE_CAPTURED",
            artifact: prodArtifact.name,
            run_conclusion: run.conclusion,
          };
        }
        // Small & fast completion (<2MB, <120s) indicates a legacy dry-run
        return null;
      }
    }
  }
  return null;
}

export async function checkAndDispatch(env, fetchImpl = fetch, now = new Date()) {
  const repo = env.GITHUB_REPO || "kienhuynh22/HCM-Traffic";
  const workflowFile = env.WORKFLOW_FILE || "capture_13paths.yml";
  const branch = env.TARGET_BRANCH || "master";
  const token = env.GITHUB_TOKEN;

  if (!token) {
    throw new Error("Missing required secret: GITHUB_TOKEN in Worker environment");
  }

  const todayVN = getVietnamDateStr(now);
  const timeVN = getVietnamTimeStr(now);
  console.log(`[GUARD] Running check at ${timeVN} ICT for date ${todayVN} on ${repo}`);

  const headers = {
    "Authorization": `Bearer ${token}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "HCM-Traffic-Cloudflare-Worker-Guard",
    "X-GitHub-Api-Version": "2022-11-28",
  };

  // 1. Query recent workflow runs
  const runsUrl = `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/runs?per_page=15`;
  const runsRes = await fetchImpl(runsUrl, { headers });

  if (!runsRes.ok) {
    const errorBody = await runsRes.text();
    throw new Error(`GitHub API query failed (HTTP ${runsRes.status}): ${errorBody}`);
  }

  const runsData = await runsRes.json();
  const runs = runsData.workflow_runs || [];

  // Check 1: Any production live run currently queued, in_progress, or waiting for today?
  for (const r of runs) {
    if (isActiveProductionLiveRun(r, todayVN)) {
      const msg = `Active production live run already exists for ${todayVN}: Run ID ${r.id} (status: ${r.status}, created: ${r.created_at}). Skipping dispatch.`;
      console.log(`[GUARD] ${msg}`);
      return { action: "skipped", reason: "ACTIVE_RUN_EXISTS", run_id: r.id, status: r.status, date: todayVN };
    }
  }

  // Check 2: Any completed run (including conclusion == 'failure' like Run #20) that captured slots today?
  const cutoffMs = now.getTime() - 48 * 3600 * 1000;
  for (const r of runs) {
    if (r.status !== "completed") {
      continue;
    }
    if (r.created_at && new Date(r.created_at).getTime() < cutoffMs) {
      continue;
    }
    const title = String(r.display_title || "");
    if (title.includes("[trial_3slots") || title.includes("dry-run")) {
      continue;
    }

    const artUrl = `https://api.github.com/repos/${repo}/actions/runs/${r.id}/artifacts`;
    const artRes = await fetchImpl(artUrl, { headers });
    if (!artRes.ok) {
      const errTxt = await artRes.text();
      throw new Error(`GitHub API artifacts query failed for run ${r.id} (HTTP ${artRes.status}): ${errTxt}`);
    }
    const artData = await artRes.json();
    const artifacts = artData.artifacts || [];

    // Only inspect /jobs when an artifact matching today's afternoon bundle exists
    const prefix = `capture-outputs-afternoon_37slots-${todayVN}-`;
    if (!artifacts.some((a) => String(a.name || "").startsWith(prefix))) {
      continue;
    }

    const jobsUrl = `https://api.github.com/repos/${repo}/actions/runs/${r.id}/jobs`;
    const jobsRes = await fetchImpl(jobsUrl, { headers });
    if (!jobsRes.ok) {
      const errTxt = await jobsRes.text();
      throw new Error(`GitHub API jobs query failed for run ${r.id} (HTTP ${jobsRes.status}): ${errTxt}`);
    }
    const jobsData = await jobsRes.json();

    const captureMatch = classifyCompletedRunCapture(r, artifacts, jobsData.jobs || [], todayVN);
    if (captureMatch) {
      const msg = `Production live session already captured for ${todayVN}: Run ID ${r.id} (${captureMatch.reason}, artifact: ${captureMatch.artifact}). Skipping dispatch.`;
      console.log(`[GUARD] ${msg}`);
      return {
        action: "skipped",
        reason: captureMatch.reason,
        run_id: r.id,
        artifact: captureMatch.artifact,
        date: todayVN,
      };
    }
  }

  // 2. No active or captured live run found: dispatch fallback run
  console.log(`[GUARD] No active or captured production run found for ${todayVN}. Dispatching ${workflowFile}...`);

  const dispatchUrl = `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/dispatches`;
  // All GitHub REST workflow_dispatch input values must be strings
  const dispatchPayload = {
    ref: branch,
    inputs: {
      mode: "afternoon_37slots",
      dry_run: "false",
      allow_init_state: "false",
      date: todayVN,
      automated: "true",
      import_external_source: "",
      import_traffic: "0",
      import_weather: "0",
    },
  };

  const dispatchRes = await fetchImpl(dispatchUrl, {
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
  return { action: "dispatched", date: todayVN, branch, inputs: dispatchPayload.inputs };
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      checkAndDispatch(env).catch((err) => {
        console.error(`[GUARD ERROR] ${err.message}`);
        throw err;
      })
    );
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/status") {
      const todayVN = getVietnamDateStr();
      const timeVN = getVietnamTimeStr();
      return new Response(
        JSON.stringify(
          {
            status: "ok",
            timezone: "Asia/Ho_Chi_Minh",
            current_time_ict: timeVN,
            target_date_ict: todayVN,
            repo: env.GITHUB_REPO,
            workflow: env.WORKFLOW_FILE,
            cron_schedules: [
              "35 8 * * * (15:35 Asia/Ho_Chi_Minh)",
              "45 8 * * * (15:45 Asia/Ho_Chi_Minh)",
            ],
          },
          null,
          2
        ),
        { headers: { "Content-Type": "application/json" } }
      );
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
