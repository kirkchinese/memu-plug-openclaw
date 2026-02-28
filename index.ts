import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";

const memuPlugin = {
  id: "memu-plugin",
  name: "memU Memory Plugin",
  kind: "memory",

  register(api: OpenClawPluginApi) {
    const pythonRoot = path.join(__dirname, "python");

    // =========================================================
    // Helper: Compute Extra Paths
    // =========================================================
    const computeExtraPaths = (pluginConfig: any, workspaceDir: string): string[] => {
      const ingestConfig = pluginConfig?.ingest || {};
      const includeDefaultPaths = ingestConfig.includeDefaultPaths !== false;
      const skillsRoot = path.join(workspaceDir, "skills").toLowerCase();

      const isSkillsPath = (p: string): boolean => {
        const normalized = p.replace(/\\/g, "/").toLowerCase();
        const skillsNormalized = skillsRoot.replace(/\\/g, "/");
        return (
          normalized === skillsNormalized ||
          normalized.startsWith(`${skillsNormalized}/`) ||
          normalized.includes("/skills/")
        );
      };

      const defaultPaths = [
        path.join(workspaceDir, "AGENTS.md"),
        path.join(workspaceDir, "SOUL.md"),
        path.join(workspaceDir, "TOOLS.md"),
        path.join(workspaceDir, "MEMORY.md"),
        path.join(workspaceDir, "HEARTBEAT.md"),
        path.join(workspaceDir, "BOOTSTRAP.md"),
        path.join(workspaceDir, "memory"),
      ];

      const extraPaths = Array.isArray(ingestConfig.extraPaths)
        ? ingestConfig.extraPaths.filter((p: unknown): p is string => typeof p === "string")
        : [];

      const combined = includeDefaultPaths ? [...defaultPaths, ...extraPaths] : extraPaths;
      const out: string[] = [];
      const seen = new Set<string>();
      for (const p of combined) {
        const key = p.trim();
        if (!key || seen.has(key)) continue;
        if (isSkillsPath(key)) continue;
        seen.add(key);
        out.push(key);
      }
      return out;
    };

    // =========================================================
    // Helper: Get Plugin Config
    // =========================================================
    const getPluginConfig = (toolCtx?: { config?: any }) => {
      if (api.pluginConfig && typeof api.pluginConfig === "object") {
        return api.pluginConfig as Record<string, unknown>;
      }
      const fullCfg = toolCtx?.config;
      const cfgFromFull = fullCfg?.plugins?.entries?.[api.id]?.config;
      if (cfgFromFull && typeof cfgFromFull === "object") {
        return cfgFromFull as Record<string, unknown>;
      }
      return {};
    };

    // =========================================================
    // Helper: Get User ID
    // =========================================================
    const getUserId = (pluginConfig: any): string => {
      const fromConfig = pluginConfig?.userId;
      if (typeof fromConfig === "string" && fromConfig.trim()) return fromConfig.trim();
      const fromEnv = process.env.MEMU_USER_ID;
      if (typeof fromEnv === "string" && fromEnv.trim()) return fromEnv.trim();
      return "default";
    };

    // =========================================================
    // Helper: Get Session Directory
    // =========================================================
    const getSessionDir = (): string => {
      const fromEnv = process.env.OPENCLAW_SESSIONS_DIR;
      if (fromEnv && fs.existsSync(fromEnv)) return fromEnv;

      const home = os.homedir();
      const candidates = [
        path.join(home, ".openclaw", "agents", "main", "sessions"),
        path.join(home, ".openclaw", "sessions"),
      ];
      for (const c of candidates) {
        if (c && fs.existsSync(c)) return c;
      }
      return candidates[0];
    };

    // =========================================================
    // Helper: Get Retrieval Config
    // =========================================================
    const getRetrievalConfig = (
      pluginConfig: any
    ): {
      mode: "fast" | "full";
      contextMessages: number;
      defaultCategoryQuota: number | null;
      defaultItemQuota: number | null;
      outputMode: "compact" | "full";
    } => {
      const retrieval = pluginConfig?.retrieval || {};
      const rawMode = typeof retrieval.mode === "string" ? retrieval.mode.toLowerCase() : "fast";
      const mode: "fast" | "full" = rawMode === "full" ? "full" : "fast";
      const rawOutputMode = typeof retrieval.outputMode === "string" ? retrieval.outputMode.toLowerCase() : "compact";
      const outputMode: "compact" | "full" = rawOutputMode === "full" ? "full" : "compact";
      const rawContext = Number(retrieval.contextMessages);
      const contextMessages = Number.isFinite(rawContext) ? Math.max(0, Math.min(20, Math.trunc(rawContext))) : 3;
      const rawDefaultCategory = Number(retrieval.defaultCategoryQuota);
      const rawDefaultItem = Number(retrieval.defaultItemQuota);
      const defaultCategoryQuota = Number.isFinite(rawDefaultCategory)
        ? Math.max(0, Math.trunc(rawDefaultCategory))
        : null;
      const defaultItemQuota = Number.isFinite(rawDefaultItem)
        ? Math.max(0, Math.trunc(rawDefaultItem))
        : null;
      return { mode, contextMessages, defaultCategoryQuota, defaultItemQuota, outputMode };
    };

    // =========================================================
    // Helper: Extract Text Content from Message
    // =========================================================
    const extractTextContent = (content: unknown): string => {
      if (typeof content === "string") return content;
      if (!Array.isArray(content)) return "";
      const parts: string[] = [];
      for (const item of content as Array<{ type?: string; text?: string }>) {
        if (item && item.type === "text" && typeof item.text === "string" && item.text.trim()) {
          parts.push(item.text);
        }
      }
      return parts.join("\n").trim();
    };

    // =========================================================
    // Helper: Get Recent Session Messages (for full retrieval mode)
    // =========================================================
    const getRecentSessionMessages = (
      sessionDir: string,
      maxMessages: number
    ): Array<{ role: "user" | "assistant"; content: string }> => {
      if (maxMessages <= 0) return [];
      try {
        let sessionId: string | undefined;
        const sessionsMetaPath = path.join(sessionDir, "sessions.json");
        if (fs.existsSync(sessionsMetaPath)) {
          try {
            const raw = fs.readFileSync(sessionsMetaPath, "utf-8");
            const parsed = JSON.parse(raw) as Record<string, { sessionId?: string }>;
            sessionId = parsed?.["agent:main:main"]?.sessionId;
            if (!sessionId) {
              const first = Object.values(parsed || {}).find(
                (v) => typeof v?.sessionId === "string" && v.sessionId
              );
              sessionId = first?.sessionId;
            }
          } catch {
            sessionId = undefined;
          }
        }

        let sessionFile = sessionId ? path.join(sessionDir, `${sessionId}.jsonl`) : "";
        if (!sessionFile || !fs.existsSync(sessionFile)) {
          const candidates = fs
            .readdirSync(sessionDir)
            .filter((f) => f.endsWith(".jsonl"))
            .map((f) => {
              const full = path.join(sessionDir, f);
              const st = fs.statSync(full);
              return { full, mtimeMs: st.mtimeMs };
            })
            .sort((a, b) => b.mtimeMs - a.mtimeMs);
          sessionFile = candidates[0]?.full || "";
        }

        if (!sessionFile || !fs.existsSync(sessionFile)) return [];
        const lines = fs.readFileSync(sessionFile, "utf-8").split("\n").filter(Boolean);
        const out: Array<{ role: "user" | "assistant"; content: string }> = [];
        for (const line of lines) {
          try {
            const evt = JSON.parse(line) as {
              type?: string;
              message?: { role?: string; content?: unknown };
            };
            if (evt?.type !== "message") continue;
            const role = evt?.message?.role;
            if (role !== "user" && role !== "assistant") continue;
            const text = extractTextContent(evt?.message?.content);
            if (!text) continue;
            out.push({ role, content: text });
          } catch {
            continue;
          }
        }

        return out.slice(-maxMessages);
      } catch {
        return [];
      }
    };

    // =========================================================
    // Helper: Get memU Data Directory
    // =========================================================
    const getMemuDataDir = (pluginConfig: any): string => {
      const fromConfig = pluginConfig?.dataDir;
      if (typeof fromConfig === "string" && fromConfig.trim()) {
        const resolved = fromConfig.startsWith("~")
          ? path.join(os.homedir(), fromConfig.slice(1))
          : fromConfig;
        return resolved;
      }
      const fromEnv = process.env.MEMU_DATA_DIR;
      if (fromEnv && fromEnv.trim()) return fromEnv;
      return path.join(os.homedir(), ".openclaw", "memUdata");
    };

    // =========================================================
    // Helper: PID File Path
    // =========================================================
    const pidFilePath = (dataDir: string) => path.join(dataDir, "watch_sync.pid");
    const dashboardPidFilePath = (dataDir: string) => path.join(dataDir, "dashboard.pid");

    // =========================================================
    // Background Service Management
    // =========================================================
    let syncProcess: ChildProcess | null = null;
    let dashboardProcess: ChildProcess | null = null;
    let dashboardRestartCount = 0;
    let isShuttingDown = false;
    let stopInProgressUntil = 0;

    const killSyncPid = (pid: number) => {
      if (!Number.isFinite(pid) || pid <= 1) return;
      if (process.platform === "win32") {
        try {
          execFileSync("taskkill", ["/PID", String(pid), "/F", "/T"], { stdio: "ignore" });
        } catch {
          // ignore — process may already be gone
        }
      } else {
        try {
          process.kill(-pid, "SIGTERM");
        } catch {
          try {
            process.kill(pid, "SIGTERM");
          } catch {
            // ignore
          }
        }
      }
    };

    const stopSyncService = (dataDir: string) => {
      isShuttingDown = true;
      stopInProgressUntil = Date.now() + 8000;

      if (syncProcess && syncProcess.pid) {
        killSyncPid(syncProcess.pid);
        syncProcess = null;
      }

      try {
        const pidPath = pidFilePath(dataDir);
        if (fs.existsSync(pidPath)) {
          const pidStr = fs.readFileSync(pidPath, "utf-8").trim();
          const pid = Number(pidStr);
          killSyncPid(pid);
          fs.unlinkSync(pidPath);
        }
      } catch {
        // ignore
      }

      const scriptPath = path.join(pythonRoot, "scripts", "watch_sync.py");

      if (process.platform !== "win32") {
        // Unix: use pkill to find by command line
        try {
          execFileSync("pkill", ["-f", scriptPath], { stdio: "ignore" });
        } catch {
          // ignore
        }
      }
      // On Windows, killSyncPid with the actual PID (above) is sufficient;
      // WINDOWTITLE-based taskkill does not work for background Python processes.

      try {
        const lockPath = path.join(os.tmpdir(), "memu_sync.lock_watch_sync");
        if (fs.existsSync(lockPath)) {
          const pid = Number(fs.readFileSync(lockPath, "utf-8").trim());
          killSyncPid(pid);
        }
      } catch {
        // ignore
      }

      isShuttingDown = false;
    };

    // =========================================================
    // Dashboard Service Management
    // =========================================================
    const stopDashboard = (dataDir: string) => {
      if (dashboardProcess && dashboardProcess.pid) {
        killSyncPid(dashboardProcess.pid);
        dashboardProcess = null;
      }
      try {
        const pidPath = dashboardPidFilePath(dataDir);
        if (fs.existsSync(pidPath)) {
          const pid = Number(fs.readFileSync(pidPath, "utf-8").trim());
          killSyncPid(pid);
          fs.unlinkSync(pidPath);
        }
      } catch {
        // ignore
      }
    };

    const startDashboard = (pluginConfig: any) => {
      if (dashboardProcess) return;

      const dataDir = getMemuDataDir(pluginConfig);
      const dashboardPort = String(
        pluginConfig.dashboardPort || process.env.MEMU_DASHBOARD_PORT || "8377"
      );

      // Kill any old dashboard process from PID file
      stopDashboard(dataDir);

      // Kill whatever is occupying the port (Windows)
      if (process.platform === "win32") {
        try {
          execFileSync("powershell", [
            "-NoProfile", "-Command",
            `Get-NetTCPConnection -LocalPort ${dashboardPort} -ErrorAction SilentlyContinue | Select-Object OwningProcess -Unique | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }`
          ], { stdio: "ignore", timeout: 5000 });
        } catch {
          // ignore
        }
      } else {
        try {
          execFileSync("fuser", ["-k", `${dashboardPort}/tcp`], { stdio: "ignore" });
        } catch {
          // ignore
        }
      }

      const env = {
        ...process.env,
        PYTHONIOENCODING: "utf-8",
        MEMU_DATA_DIR: dataDir,
        MEMU_DASHBOARD_PORT: dashboardPort,
      };

      const scriptPath = path.join(pythonRoot, "scripts", "dashboard.py");
      if (!fs.existsSync(scriptPath)) {
        console.warn(`[memU] Dashboard script not found: ${scriptPath}`);
        return;
      }

      console.log(`[memU] Starting dashboard on http://127.0.0.1:${dashboardPort}`);

      const proc = spawn("uv", ["run", "--project", pythonRoot, "python", scriptPath], {
        cwd: pythonRoot,
        env,
        stdio: "pipe",
      });

      dashboardProcess = proc;

      try {
        const pidPath = dashboardPidFilePath(dataDir);
        fs.mkdirSync(path.dirname(pidPath), { recursive: true });
        if (proc.pid) fs.writeFileSync(pidPath, String(proc.pid), "utf-8");
      } catch {
        // ignore
      }

      proc.stdout?.on("data", (d) => {
        const lines = d.toString().trim().split("\n");
        lines.forEach((l: string) => console.log(`[memU Dashboard] ${l}`));
      });
      proc.stderr?.on("data", (d) => {
        const lines = d.toString().trim().split("\n");
        lines.forEach((l: string) => console.log(`[memU Dashboard] ${l}`));
      });

      proc.on("close", (code, signal) => {
        if (dashboardProcess !== proc) return;
        dashboardProcess = null;
        try {
          const pidPath = dashboardPidFilePath(dataDir);
          if (fs.existsSync(pidPath)) fs.unlinkSync(pidPath);
        } catch {
          // ignore
        }
        if (isShuttingDown || Date.now() < stopInProgressUntil) return;
        if (code !== 0 && code !== null && !signal) {
          dashboardRestartCount++;
          if (dashboardRestartCount >= 3) {
            console.error(`[memU] Dashboard failed ${dashboardRestartCount} times, giving up. Check port ${dashboardPort} manually.`);
            return;
          }
          console.warn(`[memU] Dashboard exited (code ${code}). Restarting in 5s... (attempt ${dashboardRestartCount}/3)`);
          setTimeout(() => startDashboard(pluginConfig), 5000);
        } else {
          dashboardRestartCount = 0;
        }
      });
    };

    let lastDataDirForCleanup: string | null = null;
    let shutdownHooksInstalled = false;

    const installShutdownHooksOnce = () => {
      if (shutdownHooksInstalled) return;
      shutdownHooksInstalled = true;

      const cleanup = () => {
        if (!lastDataDirForCleanup) return;
        try {
          stopSyncService(lastDataDirForCleanup);
        } catch {
          // ignore
        }
        try {
          stopDashboard(lastDataDirForCleanup);
        } catch {
          // ignore
        }
      };

      process.once("exit", cleanup);
      process.once("SIGINT", () => {
        cleanup();
        process.exit(0);
      });
      process.once("SIGTERM", () => {
        cleanup();
        process.exit(0);
      });
    };

    const startSyncService = (pluginConfig: any, workspaceDir: string) => {
      if (syncProcess) return; // Already running

      const dataDir = getMemuDataDir(pluginConfig);
      lastDataDirForCleanup = dataDir;
      installShutdownHooksOnce();

      const embeddingConfig = pluginConfig.embedding || {};
      const extractionConfig = pluginConfig.extraction || {};
      const extraPaths = computeExtraPaths(pluginConfig, workspaceDir);
      const userId = getUserId(pluginConfig);
      const sessionDir = getSessionDir();
      const ingestConfig = pluginConfig.ingest || {};

      const env = {
        ...process.env,
        PYTHONIOENCODING: "utf-8",
        MEMU_USER_ID: userId,
        MEMU_EMBED_PROVIDER: embeddingConfig.provider || "openai",
        MEMU_EMBED_API_KEY: embeddingConfig.apiKey || process.env.MEMU_EMBED_API_KEY || "",
        MEMU_EMBED_BASE_URL: embeddingConfig.baseUrl || "https://api.openai.com/v1",
        MEMU_EMBED_MODEL: embeddingConfig.model || "text-embedding-3-small",

        MEMU_CHAT_PROVIDER: extractionConfig.provider || "openai",
        MEMU_CHAT_API_KEY: extractionConfig.apiKey || process.env.MEMU_CHAT_API_KEY || "",
        MEMU_CHAT_BASE_URL: extractionConfig.baseUrl || "https://api.openai.com/v1",
        MEMU_CHAT_MODEL: extractionConfig.model || "gpt-4o-mini",

        MEMU_DATA_DIR: dataDir,
        MEMU_WORKSPACE_DIR: workspaceDir,
        MEMU_EXTRA_PATHS: JSON.stringify(extraPaths),
        MEMU_OUTPUT_LANG: pluginConfig.language || "auto",
        OPENCLAW_SESSIONS_DIR: sessionDir,
        MEMU_USER_NAME: pluginConfig.userName || process.env.MEMU_USER_NAME || "",
        MEMU_ASSISTANT_NAME: pluginConfig.assistantName || process.env.MEMU_ASSISTANT_NAME || "",
        MEMU_FILTER_SCHEDULED_SYSTEM_MESSAGES:
          ingestConfig.filterScheduledSystemMessages === false ? "false" : "true",
        MEMU_SCHEDULED_SYSTEM_MODE:
          typeof ingestConfig.scheduledSystemMode === "string"
            ? ingestConfig.scheduledSystemMode
            : "event",
        MEMU_SCHEDULED_SYSTEM_MIN_CHARS:
          Number.isFinite(Number(ingestConfig.scheduledSystemMinChars))
            ? String(Math.max(64, Math.trunc(Number(ingestConfig.scheduledSystemMinChars))))
            : "500",
      };

      const scriptPath = path.join(pythonRoot, "scripts", "watch_sync.py");

      console.log(`[memU] Starting background sync service: ${scriptPath}`);

      const proc = spawn("uv", ["run", "--project", pythonRoot, "python", scriptPath], {
        cwd: pythonRoot,
        env,
        stdio: "pipe",
      });

      syncProcess = proc;
      isShuttingDown = false;

      // Write PID file for orphan cleanup
      try {
        const pidPath = pidFilePath(dataDir);
        fs.mkdirSync(path.dirname(pidPath), { recursive: true });
        if (syncProcess.pid) fs.writeFileSync(pidPath, String(syncProcess.pid), "utf-8");
      } catch {
        // ignore
      }

      // Redirect logs to Gateway console
      proc.stdout?.on("data", (d) => {
        const lines = d.toString().trim().split("\n");
        lines.forEach((l: string) => console.log(`[memU Sync] ${l}`));
      });
      proc.stderr?.on("data", (d) => {
        const lines = d.toString().trim().split("\n");
        lines.forEach((l: string) => console.log(`[memU Sync] ${l}`));
      });

      proc.on("close", (code, signal) => {
        if (syncProcess !== proc) return;
        syncProcess = null;
        try {
          const pidPath = pidFilePath(dataDir);
          if (fs.existsSync(pidPath)) fs.unlinkSync(pidPath);
        } catch {
          // ignore
        }
        if (isShuttingDown || Date.now() < stopInProgressUntil) return;

        if (code === 0 || signal === "SIGTERM" || signal === "SIGINT" || signal === "SIGKILL") {
          console.log(
            `[memU] Sync service exited normally (code ${code ?? "null"}, signal ${signal ?? "none"}).`
          );
          return;
        }

        if (code === null && !signal) {
          console.log("[memU] Sync service exited without code/signal; skip restart.");
          return;
        }

        if (!isShuttingDown) {
          console.warn(`[memU] Sync service crashed (code ${code}). Restarting in 5s...`);
          setTimeout(() => startSyncService(pluginConfig, workspaceDir), 5000);
        }
      });
    };

    // =========================================================
    // Auto-start Sync Service on Gateway Init
    // =========================================================
    const getGatewayManagementCommand = (): "stop" | "restart" | "status" | "health" | null => {
      const argv = process.argv.slice(2).map((v) => String(v).toLowerCase());
      if (!argv.includes("gateway")) return null;
      const mgmt: Array<"stop" | "restart" | "status" | "health"> = ["stop", "restart", "status", "health"];
      for (const c of mgmt) {
        if (argv.includes(c)) return c;
      }
      return null;
    };

    /**
     * Returns true only when OpenClaw is running as a gateway (interactive agent session).
     * All other CLI subcommands (plugins, config, upgrade, --version, etc.) should NOT
     * trigger the background sync service — it would waste resources and spam the console.
     *
     * Strategy: whitelist the known "interactive" invocations rather than trying to
     * blacklist every possible subcommand.
     *   - No subcommands at all  →  gateway mode (bare `openclaw`)
     *   - First subcommand is "gateway" →  gateway mode
     * Everything else (plugins, config, upgrade, --version, --help …) is skipped.
     */
    const isGatewayContext = (): boolean => {
      const argv = process.argv.slice(2).map((v) => String(v).toLowerCase());
      // Strip flag args (--foo, -f) to find the first real subcommand
      const subcommands = argv.filter((a) => !a.startsWith("-"));
      if (subcommands.length === 0) return true;          // bare `openclaw`
      if (subcommands[0] === "gateway") return true;      // explicit gateway
      return false;
    };

    let autoStartTriggered = false;
    const triggerAutoStart = () => {
      if (autoStartTriggered) return;
      autoStartTriggered = true;

      // Skip sync for any non-gateway CLI invocation (plugins install, config, upgrade …)
      if (!isGatewayContext()) {
        return;
      }

      const mgmtCmd = getGatewayManagementCommand();
      if (mgmtCmd) {
        if (mgmtCmd === "stop" || mgmtCmd === "restart") {
          try {
            const pluginConfig = getPluginConfig();
            const dataDir = getMemuDataDir(pluginConfig);
            stopSyncService(dataDir);
            stopDashboard(dataDir);
          } catch {
            // ignore
          }
        }
        console.log("[memU] Skipping auto-start for gateway management command.");
        return;
      }

      // Defer to next tick to ensure plugin is fully registered
      setImmediate(() => {
        try {
          const pluginConfig = getPluginConfig();
          const home = os.homedir();
          const workspaceCandidates = [
            process.env.OPENCLAW_WORKSPACE_DIR,
            path.join(home, ".openclaw", "workspace"),
            process.cwd(),
          ].filter(Boolean) as string[];

          let workspaceDir = workspaceCandidates[0];
          for (const c of workspaceCandidates) {
            if (fs.existsSync(c)) {
              workspaceDir = c;
              break;
            }
          }

          console.log(`[memU] Auto-starting sync service for workspace: ${workspaceDir}`);
          console.log(`[memU] Config - embed: ${pluginConfig?.embedding?.baseUrl}, chat: ${pluginConfig?.extraction?.baseUrl}`);
          startSyncService(pluginConfig, workspaceDir);

          // Auto-start dashboard
          if ((pluginConfig as any)?.dashboard !== false) {
            startDashboard(pluginConfig);
          }
        } catch (e) {
          console.error(`[memU] Auto-start failed: ${e}`);
        }
      });
    };

    triggerAutoStart();

    // =========================================================
    // Optional: Compaction Flush Hook
    // =========================================================
    const registerCompactionFlushHook = () => {
      const pluginConfig = api.pluginConfig || {};
      const enabled = (pluginConfig as any)?.flushOnCompaction === true;
      if (!enabled) return;

      const apiAny = api as any;
      const hookName = "after_compaction";
      const handler = async (_event: unknown, ctx: any) => {
        try {
          const workspaceDir = ctx?.workspaceDir || process.env.OPENCLAW_WORKSPACE_DIR || process.cwd();
          await runPython("flush.py", [], pluginConfig, workspaceDir);
        } catch (e) {
          console.error(`[memU] after_compaction flush failed: ${e}`);
        }
      };

      if (typeof apiAny.on === "function") {
        apiAny.on(hookName, handler, { priority: -10 });
        console.log(`[memU] Registered hook: ${hookName} (flushOnCompaction=true)`);
        return;
      }

      if (typeof apiAny.registerHook === "function") {
        apiAny.registerHook(hookName, handler, { name: "memu-plugin:after_compaction_flush" });
        console.log(`[memU] Registered hook via registerHook: ${hookName} (flushOnCompaction=true)`);
        return;
      }

      console.warn("[memU] Hook API not available; cannot enable flushOnCompaction");
    };

    // =========================================================
    // Helper: Run Python Script
    // =========================================================
    const runPython = async (
      scriptName: string,
      args: string[],
      pluginConfig: any,
      workspaceDir: string
    ): Promise<string> => {
      // Trigger background service (lazy singleton)
      startSyncService(pluginConfig, workspaceDir);

      const embeddingConfig = pluginConfig.embedding || {};
      const extractionConfig = pluginConfig.extraction || {};
      const extraPaths = computeExtraPaths(pluginConfig, workspaceDir);
      const sessionDir = getSessionDir();
      const userId = getUserId(pluginConfig);
      const ingestConfig = pluginConfig.ingest || {};

      const env = {
        ...process.env,
        PYTHONIOENCODING: "utf-8",
        MEMU_USER_ID: userId,
        MEMU_EMBED_PROVIDER: embeddingConfig.provider || "openai",
        MEMU_EMBED_API_KEY: embeddingConfig.apiKey || process.env.MEMU_EMBED_API_KEY || "",
        MEMU_EMBED_BASE_URL: embeddingConfig.baseUrl || "https://api.openai.com/v1",
        MEMU_EMBED_MODEL: embeddingConfig.model || "text-embedding-3-small",
        MEMU_CHAT_PROVIDER: extractionConfig.provider || "openai",
        MEMU_CHAT_API_KEY: extractionConfig.apiKey || process.env.MEMU_CHAT_API_KEY || "",
        MEMU_CHAT_BASE_URL: extractionConfig.baseUrl || "https://api.openai.com/v1",
        MEMU_CHAT_MODEL: extractionConfig.model || "gpt-4o-mini",
        MEMU_DATA_DIR: getMemuDataDir(pluginConfig),
        MEMU_WORKSPACE_DIR: workspaceDir,
        MEMU_EXTRA_PATHS: JSON.stringify(extraPaths),
        OPENCLAW_SESSIONS_DIR: sessionDir,
        MEMU_OUTPUT_LANG: pluginConfig.language || "auto",
        MEMU_DEBUG_TIMING: (pluginConfig as any)?.debugTiming === true ? "true" : "false",
        MEMU_USER_NAME: pluginConfig.userName || process.env.MEMU_USER_NAME || "",
        MEMU_ASSISTANT_NAME: pluginConfig.assistantName || process.env.MEMU_ASSISTANT_NAME || "",
        MEMU_FILTER_SCHEDULED_SYSTEM_MESSAGES:
          ingestConfig.filterScheduledSystemMessages === false ? "false" : "true",
        MEMU_SCHEDULED_SYSTEM_MODE:
          typeof ingestConfig.scheduledSystemMode === "string"
            ? ingestConfig.scheduledSystemMode
            : "event",
        MEMU_SCHEDULED_SYSTEM_MIN_CHARS:
          Number.isFinite(Number(ingestConfig.scheduledSystemMinChars))
            ? String(Math.max(64, Math.trunc(Number(ingestConfig.scheduledSystemMinChars))))
            : "500",
      };

      return new Promise((resolve) => {
        const proc = spawn(
          "uv",
          ["run", "--project", pythonRoot, "python", path.join(pythonRoot, "scripts", scriptName), ...args],
          { cwd: pythonRoot, env }
        );

        let stdout = "";
        let stderr = "";
        proc.stdout.on("data", (data) => {
          stdout += data.toString();
        });
        proc.stderr.on("data", (data) => {
          stderr += data.toString();
        });

        proc.on("close", (code) => {
          if (code !== 0) resolve(`Error (code ${code}): ${stderr}`);
          else resolve(stdout.trim() || "No content found.");
        });
      });
    };

    // Register hooks after helpers are available
    registerCompactionFlushHook();

    // =========================================================
    // Tool Schemas
    // =========================================================
    const searchSchema = {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        maxResults: { type: "integer", description: "Maximum number of results to return." },
        minScore: { type: "number", description: "Minimum relevance score (0.0 to 1.0)." },
        categoryQuota: { type: "integer", description: "Preferred number of category results." },
        itemQuota: { type: "integer", description: "Preferred number of item results." },
      },
      required: ["query"],
    };

    const flushSchema = {
      type: "object",
      properties: {},
      required: [],
    };

    const getSchema = {
      type: "object",
      properties: {
        path: { type: "string", description: "Path to the memory file or memU resource URL." },
        from: { type: "integer", description: "Start line (1-based)." },
        lines: { type: "integer", description: "Number of lines to read." },
      },
      required: ["path"],
    };

    // =========================================================
    // Register Tools
    // =========================================================
    api.registerTool(
      (ctx) => {
        const pluginConfig = getPluginConfig(ctx);
        const workspaceDir = ctx.workspaceDir || process.cwd();

        // memory_search tool
        const searchTool = (name: string, description: string) => ({
          name,
          description,
          parameters: searchSchema,
          async execute(_toolCallId: string, params: unknown) {
            const { query, maxResults, minScore, categoryQuota, itemQuota } = params as {
              query?: string;
              maxResults?: number;
              minScore?: number;
              categoryQuota?: number;
              itemQuota?: number;
            };
            if (!query) {
              return {
                content: [{ type: "text", text: "Missing required parameter: query" }],
                details: { error: "missing_query" },
              };
            }

            const retrievalCfg = getRetrievalConfig(pluginConfig);
            let contextCount = 0;
            const args: string[] = [query, "--mode", retrievalCfg.mode];
            if (typeof maxResults === "number" && Number.isFinite(maxResults)) {
              args.push("--max-results", String(Math.trunc(maxResults)));
            }
            if (typeof minScore === "number" && Number.isFinite(minScore)) {
              args.push("--min-score", String(minScore));
            }
            if (typeof categoryQuota === "number" && Number.isFinite(categoryQuota)) {
              args.push("--category-quota", String(Math.trunc(categoryQuota)));
            } else if (retrievalCfg.defaultCategoryQuota !== null) {
              args.push("--category-quota", String(retrievalCfg.defaultCategoryQuota));
            }
            if (typeof itemQuota === "number" && Number.isFinite(itemQuota)) {
              args.push("--item-quota", String(Math.trunc(itemQuota)));
            } else if (retrievalCfg.defaultItemQuota !== null) {
              args.push("--item-quota", String(retrievalCfg.defaultItemQuota));
            }
            if (retrievalCfg.mode === "full") {
              const sessionDir = getSessionDir();
              const history = getRecentSessionMessages(sessionDir, retrievalCfg.contextMessages);
              contextCount = history.length;
              const queries = [...history, { role: "user" as const, content: query }];
              args.push("--queries-json", JSON.stringify(queries));
            }

            const result = await runPython("search.py", args, pluginConfig, workspaceDir);
            let payload: string;
            let parsedForDetails: any = null;
            try {
              const parsed = JSON.parse(result);
              parsedForDetails = parsed;
              if (retrievalCfg.outputMode === "full") {
                payload = JSON.stringify(parsed);
              } else {
                const compactResults = Array.isArray(parsed?.results)
                  ? parsed.results.map((r: any) => ({
                      path: r?.path,
                      snippet: r?.snippet,
                    }))
                  : [];
                payload = JSON.stringify({ results: compactResults });
              }
            } catch {
              payload = JSON.stringify({
                results: [],
                provider: "openai",
                model: "unknown",
                fallback: null,
                citations: "off",
                error: result,
              });
            }
            return {
              content: [{ type: "text", text: payload }],
              details: {
                query,
                maxResults,
                minScore,
                categoryQuota,
                itemQuota,
                defaultCategoryQuota: retrievalCfg.defaultCategoryQuota,
                defaultItemQuota: retrievalCfg.defaultItemQuota,
                mode: retrievalCfg.mode,
                outputMode: retrievalCfg.outputMode,
                contextCount,
                contextMessages: retrievalCfg.contextMessages,
                resultCount: Array.isArray(parsedForDetails?.results)
                  ? parsedForDetails.results.length
                  : undefined,
                provider: parsedForDetails?.provider,
                model: parsedForDetails?.model,
              },
            };
          },
        });

        // memory_get tool
        const getTool = (name: string, description: string) => ({
          name,
          description,
          parameters: getSchema,
          async execute(_toolCallId: string, params: unknown) {
            const { path: memoryPath, from, lines } = params as {
              path?: string;
              from?: number;
              lines?: number;
            };
            if (!memoryPath) {
              return {
                content: [{ type: "text", text: "Missing required parameter: path" }],
                details: { error: "missing_path" },
              };
            }

            const args: string[] = [memoryPath];
            if (typeof from === "number" && Number.isFinite(from)) {
              args.push("--from", String(Math.trunc(from)));
            }
            if (typeof lines === "number" && Number.isFinite(lines)) {
              args.push("--lines", String(Math.trunc(lines)));
            }

            const result = await runPython("get.py", args, pluginConfig, workspaceDir);
            let payload: string;
            try {
              const parsed = JSON.parse(result);
              payload = JSON.stringify(parsed);
            } catch {
              payload = JSON.stringify({
                path: memoryPath,
                text: "",
                error: result,
              });
            }
            return {
              content: [{ type: "text", text: payload }],
              details: { path: memoryPath },
            };
          },
        });

        return [
          searchTool("memu_search", "Agentic semantic search on the memU long-term database."),
          searchTool("memory_search", "Mandatory recall step: semantically search the memory system."),
          getTool("memu_get", "Retrieve content from memU database or workspace disk."),
          getTool("memory_get", "Read a specific memory Markdown file."),
          {
            name: "memory_flush",
            description: "Force-finalize (freeze) the staged conversation tail and trigger memU ingestion immediately.",
            parameters: flushSchema,
            async execute(_toolCallId: string) {
              const result = await runPython("flush.py", [], pluginConfig, workspaceDir);
              return {
                content: [{ type: "text", text: result }],
                details: { action: "flush" },
              };
            },
          },
          {
            name: "memu_flush",
            description: "Alias of memory_flush.",
            parameters: flushSchema,
            async execute(_toolCallId: string) {
              const result = await runPython("flush.py", [], pluginConfig, workspaceDir);
              return {
                content: [{ type: "text", text: result }],
                details: { action: "flush" },
              };
            },
          },
        ];
      },
      {
        names: [
          "memu_search",
          "memory_search",
          "memu_get",
          "memory_get",
          "memory_flush",
          "memu_flush",
        ],
      }
    );

    console.log("[INFO] memU plugin registered successfully");
  },
};

export default memuPlugin;