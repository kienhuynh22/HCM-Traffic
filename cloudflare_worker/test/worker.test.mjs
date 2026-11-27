import test from "node:test";
import assert from "node:assert/strict";
import worker, {
  ACTIVE_RUN_STATUSES,
  getVietnamDateStr,
  isActiveProductionLiveRun,
  classifyCompletedRunCapture,
  checkAndDispatch,
} from "../src/index.js";

const ENV = {
  GITHUB_REPO: "kienhuynh22/HCM-Traffic",
  WORKFLOW_FILE: "capture_13paths.yml",
  TARGET_BRANCH: "master",
  GITHUB_TOKEN: "ghp_mock_token",
};

function jsonResponse(body, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() {
      return body;
    },
    async text() {
      return JSON.stringify(body);
    },
  };
}

test("Run #20 (2026-11-26): 36/37 captured with GitHub conclusion=failure blocks recapture", async () => {
  const now = new Date("2026-11-26T12:10:00Z"); // 19:10 ICT on 2026-11-26
  let dispatched = false;

  const mockFetch = async (url, init) => {
    if (init?.method === "POST") {
      dispatched = true;
      return jsonResponse({}, 204);
    }
    if (url.includes("/workflows/capture_13paths.yml/runs")) {
      return jsonResponse({
        workflow_runs: [
          {
            id: 45788636369,
            status: "completed",
            conclusion: "failure",
            event: "workflow_dispatch",
            display_title: "Capture 13 Paths",
            created_at: "2026-11-26T09:04:42Z",
          },
        ],
      });
    }
    if (url.includes("/runs/45788636369/artifacts")) {
      return jsonResponse({
        artifacts: [
          {
            name: "capture-outputs-afternoon_37slots-2026-11-26-run45788636369",
            size_in_bytes: 152005006,
          },
        ],
      });
    }
    if (url.includes("/runs/45788636369/jobs")) {
      return jsonResponse({
        jobs: [
          {
            steps: [
              {
                name: "Execute Capture Run (Trial or Afternoon)",
                conclusion: "success",
                started_at: "2026-11-26T09:05:16Z",
                completed_at: "2026-11-26T12:00:17Z",
              },
            ],
          },
        ],
      });
    }
    throw new Error(`Unexpected URL: ${url}`);
  };

  const res = await checkAndDispatch(ENV, mockFetch, now);
  assert.equal(dispatched, false);
  assert.equal(res.action, "skipped");
  assert.equal(res.reason, "PARTIAL_LIVE_CAPTURED");
  assert.equal(res.run_id, 45788636369);
});

test("Dry-run, trial_3slots, and midnight Run #21 (01:37 ICT) do NOT block afternoon dispatch", async () => {
  const now1535 = new Date("2026-11-27T08:35:00Z"); // 15:35 ICT on 2026-11-27
  let dispatchBody = null;

  const mockFetch = async (url, init) => {
    if (init?.method === "POST") {
      dispatchBody = JSON.parse(init.body);
      return jsonResponse({}, 204);
    }
    if (url.includes("/workflows/capture_13paths.yml/runs")) {
      return jsonResponse({
        workflow_runs: [
          // Active dry-run
          {
            id: 50003,
            status: "in_progress",
            conclusion: null,
            event: "workflow_dispatch",
            display_title: "Capture 13 Paths [afternoon_37slots dry-run 2026-11-27]",
            created_at: "2026-11-27T08:34:00Z",
          },
          // Active trial_3slots
          {
            id: 50002,
            status: "queued",
            conclusion: null,
            event: "workflow_dispatch",
            display_title: "Capture 13 Paths [trial_3slots live auto-vn]",
            created_at: "2026-11-27T08:33:00Z",
          },
          // Midnight timeout Run #21 (cancelled at 05:37 ICT before 16:00 slot 1)
          {
            id: 45877394268,
            status: "completed",
            conclusion: "cancelled",
            event: "schedule",
            display_title: "Capture 13 Paths",
            created_at: "2026-11-26T18:37:19Z",
          },
        ],
      });
    }
    if (url.includes("/runs/45877394268/artifacts")) {
      return jsonResponse({
        artifacts: [
          {
            name: "capture-outputs-afternoon_37slots-2026-11-27-run45877394268",
            size_in_bytes: 917955,
          },
        ],
      });
    }
    if (url.includes("/runs/45877394268/jobs")) {
      return jsonResponse({
        jobs: [
          {
            steps: [
              {
                name: "Execute Capture Run (Trial or Afternoon)",
                conclusion: "cancelled",
                started_at: "2026-11-26T18:37:52Z",
                completed_at: "2026-11-26T22:37:34Z",
              },
            ],
          },
        ],
      });
    }
    throw new Error(`Unexpected URL: ${url}`);
  };

  const res = await checkAndDispatch(ENV, mockFetch, now1535);
  assert.equal(res.action, "dispatched");
  assert.equal(res.date, "2026-11-27");
  assert.deepEqual(dispatchBody, {
    ref: "master",
    inputs: {
      mode: "afternoon_37slots",
      dry_run: "false",
      allow_init_state: "false",
      date: "2026-11-27",
      automated: "true",
      import_external_source: "",
      import_traffic: "0",
      import_weather: "0",
    },
  });
});

test("Dual triggers at 15:35 and 15:45 ICT: 15:35 dispatches once, 15:45 skips active run", async () => {
  const runsState = [];
  let dispatchCount = 0;

  const mockFetch = async (url, init) => {
    if (init?.method === "POST") {
      dispatchCount += 1;
      runsState.unshift({
        id: 60001,
        status: "in_progress",
        conclusion: null,
        event: "workflow_dispatch",
        display_title: "Capture 13 Paths [afternoon_37slots live 2026-11-27]",
        created_at: "2026-11-27T08:35:02Z",
      });
      return jsonResponse({}, 204);
    }
    return jsonResponse({ workflow_runs: runsState });
  };

  const res1535 = await checkAndDispatch(ENV, mockFetch, new Date("2026-11-27T08:35:00Z"));
  assert.equal(res1535.action, "dispatched");
  assert.equal(dispatchCount, 1);

  const res1545 = await checkAndDispatch(ENV, mockFetch, new Date("2026-11-27T08:45:00Z"));
  assert.equal(res1545.action, "skipped");
  assert.equal(res1545.reason, "ACTIVE_RUN_EXISTS");
  assert.equal(dispatchCount, 1);
});

test("Manual session queued/pending/in_progress suppresses both 15:35 and 15:45 triggers", async () => {
  for (const status of ACTIVE_RUN_STATUSES) {
    for (const title of [
      "Capture 13 Paths", // Pre-merge manual run (like Run #22)
      "Capture 13 Paths [afternoon_37slots live auto-vn]", // Post-merge manual run without explicit date
      "Capture 13 Paths [afternoon_37slots live 2026-11-27]", // Post-merge manual run with explicit date
    ]) {
      assert.equal(
        isActiveProductionLiveRun(
          {
            id: 45969183827,
            status,
            event: "workflow_dispatch",
            display_title: title,
            created_at: "2026-11-27T08:32:00Z",
          },
          "2026-11-27"
        ),
        true,
        `Expected status=${status} title=${title} to be recognized as active production live run`
      );
    }
  }
});

test("HTTP requests to /check and /trigger return 404 and never call GitHub API", async () => {
  const originalFetch = globalThis.fetch;
  let githubCalls = 0;
  globalThis.fetch = async (url) => {
    githubCalls += 1;
    throw new Error(`Unexpected outbound fetch to ${url}`);
  };

  try {
    for (const [method, path] of [
      ["GET", "/check"],
      ["POST", "/check"],
      ["GET", "/trigger"],
      ["POST", "/trigger"],
    ]) {
      const req = new Request(`https://example.workers.dev${path}`, { method });
      const res = await worker.fetch(req, ENV, {});
      assert.equal(res.status, 404, `Expected 404 for ${method} ${path}`);
    }

    // Read-only GET /status succeeds without making any outbound GitHub calls
    const statusRes = await worker.fetch(new Request("https://example.workers.dev/status"), ENV, {});
    assert.equal(statusRes.status, 200);
    const body = await statusRes.json();
    assert.equal(body.status, "ok");
    assert.deepEqual(body.cron_schedules, [
      "35 8 * * * (15:35 Asia/Ho_Chi_Minh)",
      "45 8 * * * (15:45 Asia/Ho_Chi_Minh)",
    ]);
    assert.equal(githubCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
