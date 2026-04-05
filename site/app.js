const STORAGE_KEY = "wilberflow-profiles-v2";
const LAST_PROFILE_KEY = "wilberflow-last-profile-v2";
const DEFAULT_PROFILE = "Default";
const SENSITIVE_FIELDS = new Set(["qq_imap_auth_code"]);
const LEGACY_WORKSPACE_ROOTS = new Set(["/data/Wilber/workflow_demo"]);
const FALLBACK_STAGE_SEQUENCES = {
  run_all: [
    { key: "events", label: "搜索事件" },
    { key: "stations", label: "筛选台站" },
    { key: "requests", label: "生成请求" },
    { key: "mail", label: "检查邮件" },
    { key: "download", label: "下载数据" },
    { key: "extract", label: "解压数据" },
    { key: "response", label: "去仪器响应" },
    { key: "deliver", label: "整理交付" },
  ],
  resume_from_mail: [
    { key: "mail", label: "检查邮件" },
    { key: "download", label: "下载数据" },
    { key: "extract", label: "解压数据" },
    { key: "response", label: "去仪器响应" },
    { key: "deliver", label: "整理交付" },
  ],
};

const DEFAULT_SETTINGS = {
  event_dataset: "custom",
  event_start_utc: "1996-01-01T00:00",
  event_end_utc: "1996-01-31T23:59",
  min_magnitude: "6.0",
  max_magnitude: "8.5",
  min_depth_km: "0",
  max_depth_km: "700",
  latitude_range: "-60, 60",
  longitude_range: "-180, 180",
  limit_events: "3",
  selected_event_tokens: "",
  metadata_only: false,
  network_patterns: "",
  station_patterns: "",
  channel_patterns: "BH?",
  location_priority: "00,--,10",
  min_distance_deg: "30",
  max_distance_deg: "95",
  min_azimuth_deg: "-180",
  max_azimuth_deg: "180",
  request_user: "Your Name",
  request_email: "your_mail@qq.com",
  request_label_prefix: "wilberflow_batch",
  before_arrival_sec: "120",
  window_start_phase: "P",
  after_arrival_sec: "300",
  window_end_phase: "P",
  output_format: "sacbl",
  bundle: "tar",
  mail_poll_interval_sec: "30",
  mail_timeout_min: "15",
  qq_imap_auth_code: "",
  submit_requests: false,
  workspace_root: "",
  batch_mode: "new",
  batch_id: "",
  pre_filt: "0.002,0.005,2.0,4.0",
  response_backend: "local_sac_first",
};

const FIELD_HELP = {
  profile_select: "切换已保存的配置档。多人共用浏览器时，可以分别保存自己的默认值。",
  profile_name: "输入新的配置档名字后点“保存配置档”，会把当前整套参数存成独立 profile。",
  event_dataset: "先选 Wilber 事件数据集；如果选 Custom Query，再按下面的时间和阈值手动填写。",
  event_start_utc: "填 UTC 开始时间，例如 1996-01-01 00:00。只在 Custom Query 或你想覆盖默认范围时使用。",
  event_end_utc: "填 UTC 结束时间，例如 1996-01-31 23:59。只在 Custom Query 或你想覆盖默认范围时使用。",
  min_magnitude: "最小震级。Wilber 预设数据集会自动带一个默认值，你也可以手动改。",
  max_magnitude: "最大震级；常见可填 8.5 或 9.9。",
  min_depth_km: "事件最小深度，单位 km；不限制时可填 0。",
  max_depth_km: "事件最大深度，单位 km；常用值是 700。",
  latitude_range: "格式是“最小纬度, 最大纬度”，例如 -60, 60。",
  longitude_range: "格式是“最小经度, 最大经度”，例如 -180, 180。",
  limit_events: "本轮最多处理多少个事件；填 0 表示不限制。建议先用 1 到 3 做测试。",
  selected_event_tokens: "这里是最终会参与流程的事件名单。点击“搜索 Wilber 事件”后，返回结果会默认全选并自动写入这里；你取消勾选某些事件时，这里会同步减少。你也可以直接在文本框里手动增删事件。",
  selected_regions: "这里列出当前搜索结果里出现的所有地区名称，默认全选。你可以只保留本轮想处理的地区。",
  metadata_only: "勾选后，本轮只生成事件 CSV 和台站 CSV，不提交 Wilber 请求、不查邮件、不下载数据，也不做去响应和最终整理。适合先批量拿事件与台站对应关系做筛查。",
  network_patterns: "留空表示每个事件都不限制台网，也就是该事件可用台网全部参与，不只限于目录里的 500 个常见台网。下拉目录只是为了方便快速勾选常见台网。",
  station_patterns: "台站支持写 STA 或 NET.STA，也支持通配符，例如 ANMO、COLA、A*。留空表示不过滤。",
  channel_patterns: "通道会按 Wilber 的原始分类提供，例如 BH?、BHZ、LH?、LHZ。",
  location_priority: "按优先级填写 location code，例如 00,--,10。越靠前越优先。",
  min_distance_deg: "台站到事件的最小震中距，单位度，例如 30 或 35。",
  max_distance_deg: "台站到事件的最大震中距，单位度，例如 95。",
  min_azimuth_deg: "最小方位角，单位度。默认 -180，表示不限制下边界。",
  max_azimuth_deg: "最大方位角，单位度。默认 180，表示不限制上边界。",
  request_user: "Wilber 表单里的用户名。",
  request_email: "同一个邮箱同时用于 Wilber 表单接收 [Success] 邮件，以及本地 QQ IMAP 轮询。这里直接填你的 QQ 邮箱地址即可。",
  request_label_prefix: "请求标签前缀会在真正运行时自动追加当前批次号，所以默认不需要你每次手动改。它主要用来区分不同项目或不同人的任务前缀。",
  window_start_phase: "窗口起点参考的相位，常用 P。",
  before_arrival_sec: "窗口起点相对相位提前多少秒。这里使用 Wilber 常用离散选项。",
  window_end_phase: "窗口终点参考的相位，常用 P。",
  after_arrival_sec: "窗口终点相对相位延后多少秒。这里使用 Wilber 常用离散选项。",
  output_format: "这里直接对应 Wilber 表单里的 Output Format，下拉选项按当前官方页面整理，例如 sacbl、sacbb、saca、miniseed、GeoCSV 等。若要继续自动解压、去响应和整理事件，建议使用 sacbl / sacbb / saca。",
  bundle: "这里直接对应 Wilber 表单里的 Bundle As。当前官方页面支持 individual files 和 tar archive。",
  mail_poll_interval_sec: "邮箱轮询间隔，单位秒。常用 30。",
  mail_timeout_min: "等待全部 [Success] 邮件的最长时间，单位分钟。默认 15 分钟，更适合先做批量测试。",
  qq_imap_auth_code: "QQ 邮箱 IMAP 授权码。只用于当前运行，不会被页面长期保存。",
  submit_requests: "勾选后，点击“提交并开始流程”时会真的向 Wilber 提交请求，然后继续查邮件、下载和后处理。不勾选时，点击运行只会生成到 request plan，不会真正提交。",
  workspace_root: "官方只支持在 WSL/Linux 里运行本地服务。这里默认会自动填成当前项目目录下的 output/。如果你想把流程文件直接放到 Windows 盘，可在 WSL 里填 /mnt/d/...；从网页直接填 D:\\... 也会自动换算。",
  batch_mode: "默认使用“自动新建批次”，每次点击提交都会在工作根目录下新建一个批次子目录，避免把新旧事件混在一起。只有当你想继续旧任务时，才切到“继续已有批次”。",
  batch_id: "新批次模式下这里可以留空，提交时会自动生成批次号；如果你想自定义批次目录名，也可以手填。继续已有批次时，这里要填已有批次号，或从下方建议名单中选择。",
  pre_filt: "预滤波参数必须是 4 个数字，例如 0.002,0.005,2.0,4.0。",
  response_backend: "local_sac_first 表示优先用本机 SAC + SACPZ 去响应；缺少 SACPZ 时会自动尝试 EarthScope/IRIS 响应并缓存 StationXML。obspy_only 表示直接全部走 ObsPy 响应处理。",
};

const form = document.getElementById("settings-form");
const profileSelect = document.getElementById("profile-select");
const profileNameInput = document.getElementById("profile-name");
const storageStatus = document.getElementById("storage-status");
const importConfigButton = document.getElementById("import-config");
const importConfigFileInput = document.getElementById("import-config-file");
const eventDatasetSelect = document.getElementById("event-dataset");
const searchEventsButton = document.getElementById("search-events");
const eventSearchStatus = document.getElementById("event-search-status");
const eventResults = document.getElementById("event-results");
const selectedEventsStatus = document.getElementById("selected-events-status");
const regionFilterSearch = document.getElementById("region-filter-search");
const regionFilterList = document.getElementById("region-filter-list");
const regionFilterStatus = document.getElementById("region-filter-status");
const regionFilterSummary = document.getElementById("region-filter-summary");
const regionSelectVisibleButton = document.getElementById("region-select-visible");
const regionClearSelectionButton = document.getElementById("region-clear-selection");
const regionCloseDropdownButton = document.getElementById("region-close-dropdown");
const beforeArrivalSelect = document.getElementById("before-arrival-select");
const afterArrivalSelect = document.getElementById("after-arrival-select");
const networkCatalogSearch = document.getElementById("network-catalog-search");
const networkCatalogStatus = document.getElementById("network-catalog-status");
const networkCatalogList = document.getElementById("network-catalog-list");
const networkCatalogDropdown = document.getElementById("network-catalog-dropdown");
const networkCatalogSummary = document.getElementById("network-catalog-summary");
const networkSelectVisibleButton = document.getElementById("network-select-visible");
const networkClearSelectionButton = document.getElementById("network-clear-selection");
const networkCloseDropdownButton = document.getElementById("network-close-dropdown");
const runWorkflowButton = document.getElementById("run-workflow");
const resumeMailWorkflowButton = document.getElementById("resume-mail-workflow");
const workflowStatus = document.getElementById("workflow-status");
const workflowStagebar = document.getElementById("workflow-stagebar");
const batchCatalogRefreshButton = document.getElementById("batch-catalog-refresh");
const batchCatalogStatus = document.getElementById("batch-catalog-status");
const batchTargetStatus = document.getElementById("batch-target-status");
const batchIdInput = document.getElementById("batch-id");
const batchIdSelect = document.getElementById("batch-id-select");

let memoryFallback = null;
let activeProfileName = DEFAULT_PROFILE;
let floatingHelpTooltip = null;
let studioMetadata = null;
let latestEventResults = [];
let networkCatalogEntries = [];
let networkCatalogLoaded = false;
let latestRegionEntries = [];
let workflowStatusTimer = null;
let batchCatalogEntries = [];

const FIELD_NOTE_IDS = {
  workspace_root: "workspace-root-note",
  batch_mode: "batch-mode-note",
  batch_id: "batch-id-note",
  request_email: "request-email-note",
  qq_imap_auth_code: "qq-imap-auth-code-note",
  output_format: "output-format-note",
};

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function safeLocalStorageGet(key) {
  try {
    return window.localStorage.getItem(key);
  } catch (error) {
    return null;
  }
}

function safeLocalStorageSet(key, value) {
  try {
    window.localStorage.setItem(key, value);
    return true;
  } catch (error) {
    return false;
  }
}

function loadStore() {
  const raw = safeLocalStorageGet(STORAGE_KEY);
  if (!raw) {
    if (!memoryFallback) {
      memoryFallback = { profiles: { [DEFAULT_PROFILE]: clone(DEFAULT_SETTINGS) } };
    }
    return memoryFallback;
  }
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !parsed.profiles) {
      throw new Error("invalid store");
    }
    return parsed;
  } catch (error) {
    return { profiles: { [DEFAULT_PROFILE]: clone(DEFAULT_SETTINGS) } };
  }
}

function saveStore(store) {
  const payload = JSON.stringify(store);
  const persisted = safeLocalStorageSet(STORAGE_KEY, payload);
  if (!persisted) {
    memoryFallback = store;
  }
  return persisted;
}

function loadLastProfileName() {
  return safeLocalStorageGet(LAST_PROFILE_KEY) || DEFAULT_PROFILE;
}

function saveLastProfileName(name) {
  safeLocalStorageSet(LAST_PROFILE_KEY, name);
}

function listProfiles(store) {
  return Object.keys(store.profiles).sort((left, right) => {
    if (left === DEFAULT_PROFILE) {
      return -1;
    }
    if (right === DEFAULT_PROFILE) {
      return 1;
    }
    return left.localeCompare(right, "zh-CN");
  });
}

function applySettings(settings) {
  const merged = { ...clone(DEFAULT_SETTINGS), ...settings };
  form.querySelectorAll("[name]").forEach((element) => {
    const value = merged[element.name];
    if (element.type === "checkbox") {
      element.checked = Boolean(value);
    } else {
      element.value = value ?? "";
    }
  });
}

function collectFlatSettings(options = {}) {
  const { includeSensitive = false } = options;
  const settings = {};
  form.querySelectorAll("[name]").forEach((element) => {
    if (!includeSensitive && SENSITIVE_FIELDS.has(element.name)) {
      return;
    }
    settings[element.name] = element.type === "checkbox" ? element.checked : element.value.trim();
  });
  return settings;
}

function defaultWorkspaceRoot() {
  return studioMetadata?.defaults?.workspace_root || DEFAULT_SETTINGS.workspace_root || "";
}

function effectiveWorkspaceRoot(rawValue) {
  const value = String(rawValue || "").trim();
  return value || defaultWorkspaceRoot();
}

function shouldAdoptWorkspaceRoot(currentValue) {
  const value = String(currentValue || "").trim();
  return !value || LEGACY_WORKSPACE_ROOTS.has(value);
}

function applyWorkspaceRootDefault() {
  const field = form?.elements?.workspace_root;
  const fallback = defaultWorkspaceRoot();
  if (!field || !fallback) {
    return;
  }
  field.placeholder = fallback;
  if (shouldAdoptWorkspaceRoot(field.value)) {
    field.value = fallback;
  }
}

function noteElement(name) {
  const id = FIELD_NOTE_IDS[name];
  return id ? document.getElementById(id) : null;
}

function clearFieldReminder(name) {
  const note = noteElement(name);
  if (note) {
    note.textContent = "";
    note.classList.remove("has-reminder");
  }
  const field = form?.elements?.[name];
  const label = field?.closest?.("label");
  label?.classList?.remove("has-reminder");
}

function setFieldReminder(name, message) {
  const note = noteElement(name);
  if (note) {
    note.textContent = message;
    note.classList.add("has-reminder");
  }
  const field = form?.elements?.[name];
  const label = field?.closest?.("label");
  label?.classList?.add("has-reminder");
}

function clearAllFieldReminders() {
  Object.keys(FIELD_NOTE_IDS).forEach(clearFieldReminder);
}

function defaultBatchId() {
  return studioMetadata?.defaults?.batch_id || "";
}

function effectiveBatchMode(rawValue) {
  return String(rawValue || "").trim() === "existing" ? "existing" : "new";
}

function effectiveBatchId(rawValue) {
  return String(rawValue || "").trim();
}

function batchTargetWorkspace(flat) {
  const baseRoot = effectiveWorkspaceRoot(flat.workspace_root);
  const batchMode = effectiveBatchMode(flat.batch_mode);
  const batchId = effectiveBatchId(flat.batch_id);
  if (!baseRoot) {
    return "";
  }
  if (batchMode === "existing") {
    return batchId ? `${baseRoot}/${batchId}` : `${baseRoot}/<select-batch>`;
  }
  return batchId ? `${baseRoot}/${batchId}` : `${baseRoot}/<auto-new-batch>`;
}

function refreshBatchStatus(flat = collectFlatSettings()) {
  const batchMode = effectiveBatchMode(flat.batch_mode);
  const batchId = effectiveBatchId(flat.batch_id);
  const generatedExample = defaultBatchId();
  const target = batchTargetWorkspace(flat);
  const batchLabel = batchIdInput?.closest("label");
  if (batchIdSelect) {
    batchIdSelect.hidden = batchMode !== "existing";
    batchIdSelect.disabled = batchMode !== "existing";
  }
  if (batchLabel) {
    batchLabel.classList.toggle("is-muted", batchMode !== "existing");
  }
  if (form.elements.batch_id) {
    form.elements.batch_id.placeholder = batchMode === "existing"
      ? "例如 wf_20260405_152030"
      : `留空则自动生成，例如 ${generatedExample || "wf_YYYYMMDD_HHMMSS"}`;
    form.elements.batch_id.setAttribute("list", batchMode === "existing" ? "batch-catalog-options" : "");
  }
  const batchModeNote = noteElement("batch_mode");
  if (batchModeNote && !batchModeNote.classList.contains("has-reminder")) {
    batchModeNote.textContent = batchMode === "existing"
      ? "切到旧批次模式后，主按钮不会新建目录，而是继续你选中的旧批次。"
      : "默认推荐新批次模式。每次新提交都会生成新的批次目录，更安全。";
  }
  const batchIdNote = noteElement("batch_id");
  if (batchIdNote && !batchIdNote.classList.contains("has-reminder")) {
    batchIdNote.textContent = batchMode === "existing"
      ? "优先从上方下拉直接选已有批次；如果下拉里没有，也可以手动输入批次号。"
      : "新批次模式下这里可以留空自动生成；如果你想自定义目录名，也可以手动填写。";
  }
  if (batchTargetStatus) {
    batchTargetStatus.textContent = batchMode === "existing"
      ? (batchId ? `将继续已有批次：${target}` : "请选择一个已有批次号，再执行补跑或继续旧批次。")
      : (batchId ? `将新建或使用自定义新批次：${target}` : `将自动新建批次子目录：${target}`);
  }
  if (batchCatalogStatus) {
    if (batchMode === "existing") {
      batchCatalogStatus.textContent = batchCatalogEntries.length
        ? `已发现 ${batchCatalogEntries.length} 个已有批次，可直接输入或从浏览器建议中选择。`
        : "当前根目录下还没有发现可继续的旧批次，可点“刷新已有批次”。";
    } else {
      batchCatalogStatus.textContent = "默认使用新批次模式。每次提交新任务都会进入新的批次子目录，避免和旧任务混在一起。";
    }
  }
}

function splitLooseList(value) {
  return String(value || "")
    .split(/[\n,;，；]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function uniqueList(values) {
  const seen = new Set();
  const ordered = [];
  values.forEach((item) => {
    if (seen.has(item)) {
      return;
    }
    seen.add(item);
    ordered.push(item);
  });
  return ordered;
}

function csvList(value) {
  return uniqueList(splitLooseList(value));
}

function newlineList(value) {
  return uniqueList(
    String(value || "")
      .split(/\r?\n+/)
      .map((item) => item.trim())
      .filter(Boolean),
  );
}

function numberValue(value) {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function numericList(value) {
  return splitLooseList(value)
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item));
}

function rangePair(value, fallback) {
  const items = numericList(value);
  if (items.length >= 2) {
    return [items[0], items[1]];
  }
  return fallback;
}

function asInteger(value, fallback) {
  const numeric = numberValue(value);
  if (numeric === null) {
    return fallback;
  }
  return Math.round(numeric);
}

function optionalLimitValue(value) {
  const numeric = numberValue(value);
  if (numeric === null) {
    return null;
  }
  const rounded = Math.round(numeric);
  return rounded <= 0 ? null : rounded;
}

function tomlLimitValue(value, fallback) {
  const numeric = asInteger(value, fallback);
  if (numeric === null) {
    return 0;
  }
  return numeric <= 0 ? 0 : numeric;
}

function secondsToMinutes(value, fallback) {
  const numeric = numberValue(value);
  if (numeric === null) {
    return fallback;
  }
  return Math.max(0, Math.round(numeric / 60));
}

function localDateTimeToIso(value, fallback) {
  if (!value) {
    return fallback;
  }
  return value.length === 16 ? `${value}:00` : value;
}

function tomlString(value) {
  return `"${String(value ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')}"`;
}

function tomlArray(values) {
  if (!values.length) {
    return "[]";
  }
  return `[${values.map((item) => tomlString(item)).join(", ")}]`;
}

function sanitizeName(value) {
  return (
    value
      .trim()
      .replace(/[^\w.-]+/g, "_")
      .replace(/_+/g, "_")
      .replace(/^_+|_+$/g, "") || "profile"
  );
}

function basenameWithoutExtension(filename) {
  const raw = String(filename || "").trim();
  if (!raw) {
    return "";
  }
  return raw.replace(/\.[^.]+$/u, "");
}

function isoToLocalDateTime(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const match = text.match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})/);
  return match ? `${match[1]}T${match[2]}` : "";
}

function stringListFromValue(value) {
  if (Array.isArray(value)) {
    return uniqueList(value.map((item) => String(item || "").trim()).filter(Boolean));
  }
  return csvList(value);
}

function parseTomlScalar(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "";
  }
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    const body = value.slice(1, -1);
    return value.startsWith('"')
      ? body.replace(/\\"/g, '"').replace(/\\\\/g, "\\")
      : body;
  }
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  if (value.startsWith("[") && value.endsWith("]")) {
    const body = value.slice(1, -1).trim();
    if (!body) {
      return [];
    }
    return body
      .split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/)
      .map((item) => parseTomlScalar(item));
  }
  const numeric = Number(value);
  if (!Number.isNaN(numeric)) {
    return numeric;
  }
  return value;
}

function parseSimpleToml(text) {
  const root = {};
  let section = root;
  String(text || "")
    .split(/\r?\n/)
    .forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) {
        return;
      }
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        const parts = trimmed.slice(1, -1).split(".").map((item) => item.trim()).filter(Boolean);
        section = root;
        parts.forEach((part) => {
          if (!section[part] || typeof section[part] !== "object" || Array.isArray(section[part])) {
            section[part] = {};
          }
          section = section[part];
        });
        return;
      }
      const separator = trimmed.indexOf("=");
      if (separator <= 0) {
        return;
      }
      const key = trimmed.slice(0, separator).trim();
      const value = trimmed.slice(separator + 1).trim();
      section[key] = parseTomlScalar(value);
    });
  return root;
}

function settingsFromPreviewJson(payload) {
  const settings = {};
  const eventSearch = payload?.event_search || {};
  const stationFilter = payload?.station_filter || {};
  const request = payload?.request || {};
  const mail = payload?.mail || {};
  const normalize = payload?.normalize || {};
  const query = eventSearch.query || {};

  if (eventSearch.dataset) {
    settings.event_dataset = String(eventSearch.dataset);
  }
  if (query.starttime) {
    settings.event_start_utc = isoToLocalDateTime(query.starttime);
  }
  if (query.endtime) {
    settings.event_end_utc = isoToLocalDateTime(query.endtime);
  }
  if (query.minmagnitude !== undefined && query.minmagnitude !== null) {
    settings.min_magnitude = String(query.minmagnitude);
  }
  if (query.maxmagnitude !== undefined && query.maxmagnitude !== null) {
    settings.max_magnitude = String(query.maxmagnitude);
  }
  if (query.mindepth !== undefined && query.mindepth !== null) {
    settings.min_depth_km = String(query.mindepth);
  }
  if (query.maxdepth !== undefined && query.maxdepth !== null) {
    settings.max_depth_km = String(query.maxdepth);
  }
  if (query.minlatitude !== undefined && query.maxlatitude !== undefined) {
    settings.latitude_range = `${query.minlatitude}, ${query.maxlatitude}`;
  }
  if (query.minlongitude !== undefined && query.maxlongitude !== undefined) {
    settings.longitude_range = `${query.minlongitude}, ${query.maxlongitude}`;
  }
  if (eventSearch.limit_events !== undefined && eventSearch.limit_events !== null) {
    settings.limit_events = String(eventSearch.limit_events);
  }
  if (Array.isArray(eventSearch.selected_event_tokens)) {
    settings.selected_event_tokens = eventSearch.selected_event_tokens.join("\n");
  }
  if (Array.isArray(eventSearch.selected_regions)) {
    settings.selected_regions = eventSearch.selected_regions.join("\n");
  }
  if (eventSearch.metadata_only !== undefined) {
    settings.metadata_only = Boolean(eventSearch.metadata_only);
  }

  if (stationFilter.network_patterns !== undefined) {
    settings.network_patterns = stringListFromValue(stationFilter.network_patterns).join(",");
  }
  if (stationFilter.station_patterns !== undefined) {
    settings.station_patterns = stringListFromValue(stationFilter.station_patterns).join(",");
  }
  if (stationFilter.channel_patterns !== undefined) {
    settings.channel_patterns = stringListFromValue(stationFilter.channel_patterns).join(",");
  }
  if (stationFilter.location_priority !== undefined) {
    settings.location_priority = stringListFromValue(stationFilter.location_priority).join(",");
  }
  if (stationFilter.min_distance_deg !== undefined && stationFilter.min_distance_deg !== null) {
    settings.min_distance_deg = String(stationFilter.min_distance_deg);
  }
  if (stationFilter.max_distance_deg !== undefined && stationFilter.max_distance_deg !== null) {
    settings.max_distance_deg = String(stationFilter.max_distance_deg);
  }
  if (stationFilter.min_azimuth_deg !== undefined && stationFilter.min_azimuth_deg !== null) {
    settings.min_azimuth_deg = String(stationFilter.min_azimuth_deg);
  }
  if (stationFilter.max_azimuth_deg !== undefined && stationFilter.max_azimuth_deg !== null) {
    settings.max_azimuth_deg = String(stationFilter.max_azimuth_deg);
  }

  if (request.user !== undefined && request.user !== null) {
    settings.request_user = String(request.user);
  }
  if (request.email !== undefined && request.email !== null) {
    settings.request_email = String(request.email);
  }
  if (request.request_label_prefix !== undefined && request.request_label_prefix !== null) {
    settings.request_label_prefix = String(request.request_label_prefix);
  }
  if (request.window_start_phase !== undefined && request.window_start_phase !== null) {
    settings.window_start_phase = String(request.window_start_phase);
  }
  if (request.before_arrival_sec !== undefined && request.before_arrival_sec !== null) {
    settings.before_arrival_sec = String(request.before_arrival_sec);
  }
  if (request.window_end_phase !== undefined && request.window_end_phase !== null) {
    settings.window_end_phase = String(request.window_end_phase);
  }
  if (request.after_arrival_sec !== undefined && request.after_arrival_sec !== null) {
    settings.after_arrival_sec = String(request.after_arrival_sec);
  }
  if (request.output_format !== undefined && request.output_format !== null) {
    settings.output_format = String(request.output_format);
  }
  if (request.bundle !== undefined && request.bundle !== null) {
    settings.bundle = String(request.bundle);
  }
  if (request.submit_requests !== undefined) {
    settings.submit_requests = Boolean(request.submit_requests);
  }

  if (mail.poll_interval_sec !== undefined && mail.poll_interval_sec !== null) {
    settings.mail_poll_interval_sec = String(mail.poll_interval_sec);
  }
  if (mail.timeout_min !== undefined && mail.timeout_min !== null) {
    settings.mail_timeout_min = String(mail.timeout_min);
  }

  if (normalize.pre_filt !== undefined) {
    settings.pre_filt = Array.isArray(normalize.pre_filt) ? normalize.pre_filt.join(",") : String(normalize.pre_filt || "");
  }
  if (normalize.response_backend !== undefined && normalize.response_backend !== null) {
    settings.response_backend = String(normalize.response_backend);
  }

  return settings;
}

function settingsFromTomlConfig(payload) {
  const settings = {};
  const eventSearch = payload?.event_search || {};
  const query = eventSearch.query || {};
  const request = payload?.request || {};
  const mail = payload?.mail || {};
  const normalize = payload?.normalize || {};

  settings.event_dataset = "custom";
  if (query.starttime) {
    settings.event_start_utc = isoToLocalDateTime(query.starttime);
  }
  if (query.endtime) {
    settings.event_end_utc = isoToLocalDateTime(query.endtime);
  }
  if (query.minmagnitude !== undefined) {
    settings.min_magnitude = String(query.minmagnitude);
  }
  if (query.maxmagnitude !== undefined) {
    settings.max_magnitude = String(query.maxmagnitude);
  }
  if (query.mindepth !== undefined) {
    settings.min_depth_km = String(query.mindepth);
  }
  if (query.maxdepth !== undefined) {
    settings.max_depth_km = String(query.maxdepth);
  }
  if (query.minlatitude !== undefined && query.maxlatitude !== undefined) {
    settings.latitude_range = `${query.minlatitude}, ${query.maxlatitude}`;
  }
  if (query.minlongitude !== undefined && query.maxlongitude !== undefined) {
    settings.longitude_range = `${query.minlongitude}, ${query.maxlongitude}`;
  }
  if (eventSearch.limit !== undefined) {
    settings.limit_events = String(eventSearch.limit);
  }
  if (Array.isArray(eventSearch.selected_event_tokens)) {
    settings.selected_event_tokens = eventSearch.selected_event_tokens.join("\n");
  }

  if (request.channels !== undefined) {
    settings.channel_patterns = String(request.channels);
  }
  if (request.networks !== undefined) {
    settings.network_patterns = String(request.networks);
  }
  if (request.stations !== undefined) {
    settings.station_patterns = String(request.stations);
  }
  if (request.location_priority !== undefined) {
    settings.location_priority = String(request.location_priority);
  }
  if (request.min_distance_deg !== undefined) {
    settings.min_distance_deg = String(request.min_distance_deg);
  }
  if (request.max_distance_deg !== undefined) {
    settings.max_distance_deg = String(request.max_distance_deg);
  }
  if (request.min_azimuth_deg !== undefined) {
    settings.min_azimuth_deg = String(request.min_azimuth_deg);
  }
  if (request.max_azimuth_deg !== undefined) {
    settings.max_azimuth_deg = String(request.max_azimuth_deg);
  }
  if (request.window_start_phase !== undefined) {
    settings.window_start_phase = String(request.window_start_phase);
  }
  if (request.window_start_before_min !== undefined) {
    settings.before_arrival_sec = String(Number(request.window_start_before_min) * 60);
  }
  if (request.window_end_phase !== undefined) {
    settings.window_end_phase = String(request.window_end_phase);
  }
  if (request.window_end_after_min !== undefined) {
    settings.after_arrival_sec = String(Number(request.window_end_after_min) * 60);
  }
  if (request.output_format !== undefined) {
    settings.output_format = String(request.output_format);
  }
  if (request.bundle !== undefined) {
    settings.bundle = String(request.bundle);
  }
  if (request.user !== undefined) {
    settings.request_user = String(request.user);
  }
  if (request.email !== undefined) {
    settings.request_email = String(request.email);
  }
  if (request.request_label_prefix !== undefined) {
    settings.request_label_prefix = String(request.request_label_prefix);
  }
  if (request.submit !== undefined) {
    settings.submit_requests = Boolean(request.submit);
  }
  if (request.metadata_only !== undefined) {
    settings.metadata_only = Boolean(request.metadata_only);
  }

  if (mail.poll_interval_seconds !== undefined) {
    settings.mail_poll_interval_sec = String(mail.poll_interval_seconds);
  }
  if (mail.max_wait_minutes !== undefined) {
    settings.mail_timeout_min = String(mail.max_wait_minutes);
  }

  if (normalize.pre_filt !== undefined) {
    settings.pre_filt = String(normalize.pre_filt);
  }
  if (normalize.response_backend !== undefined) {
    settings.response_backend = String(normalize.response_backend);
  }
  if (normalize.limit_events !== undefined) {
    settings.limit_events = String(normalize.limit_events);
  }

  return settings;
}

function parseImportedConfig(text, filename = "") {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    throw new Error("导入文件是空的");
  }
  const lowerName = String(filename || "").toLowerCase();
  if (lowerName.endsWith(".json") || trimmed.startsWith("{")) {
    const payload = JSON.parse(trimmed);
    return settingsFromPreviewJson(payload);
  }
  if (lowerName.endsWith(".toml") || lowerName.endsWith(".tml") || trimmed.startsWith("[")) {
    return settingsFromTomlConfig(parseSimpleToml(trimmed));
  }
  throw new Error("暂时只支持导入本页面导出的 JSON 或 config.toml");
}

function importSettingsIntoCurrentProfile(settings, sourceName = "") {
  const currentSecret = form.elements.qq_imap_auth_code.value;
  applySettings({ ...collectFlatSettings(), ...settings });
  form.elements.qq_imap_auth_code.value = currentSecret;
  clearAllFieldReminders();
  refreshUi();
  saveCurrentProfile({ announce: false });
  const importedName = basenameWithoutExtension(sourceName);
  if (importedName && profileNameInput) {
    profileNameInput.value = importedName;
  }
  setStatus(`已导入 ${sourceName || "配置文件"} 到 ${activeProfileName}`, safeLocalStorageGet(STORAGE_KEY) !== null);
}

async function handleImportConfigFile(event) {
  const file = event?.target?.files?.[0];
  if (!file) {
    return;
  }
  try {
    const text = await file.text();
    const settings = parseImportedConfig(text, file.name);
    importSettingsIntoCurrentProfile(settings, file.name);
  } catch (error) {
    setStatus(`导入失败：${error.message || error}`, safeLocalStorageGet(STORAGE_KEY) !== null);
  } finally {
    if (importConfigFileInput) {
      importConfigFileInput.value = "";
    }
  }
}

function openImportConfigPicker() {
  importConfigFileInput?.click();
}

function ensureFloatingHelpTooltip() {
  if (floatingHelpTooltip) {
    return floatingHelpTooltip;
  }
  const tooltip = document.createElement("div");
  tooltip.className = "floating-help-tooltip";
  tooltip.setAttribute("role", "tooltip");
  document.body.appendChild(tooltip);
  floatingHelpTooltip = tooltip;
  return tooltip;
}

function hideFloatingHelpTooltip() {
  if (!floatingHelpTooltip) {
    return;
  }
  floatingHelpTooltip.classList.remove("is-visible");
}

function showFloatingHelpTooltip(button, text) {
  const tooltip = ensureFloatingHelpTooltip();
  tooltip.textContent = text;
  tooltip.classList.add("is-visible");

  const rect = button.getBoundingClientRect();
  const tooltipWidth = Math.min(280, window.innerWidth - 24);
  const rightSpace = window.innerWidth - rect.right - 16;
  const leftSpace = rect.left - 16;

  let left;
  if (rightSpace >= tooltipWidth) {
    left = rect.right + 16;
  } else if (leftSpace >= tooltipWidth) {
    left = rect.left - tooltipWidth - 16;
  } else {
    left = Math.max(12, Math.min(rect.right + 16, window.innerWidth - tooltipWidth - 12));
  }

  const top = Math.max(12, Math.min(rect.top - 6, window.innerHeight - tooltip.offsetHeight - 12));
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function attachHelpTooltips() {
  document.querySelectorAll("[name], [data-help-key]").forEach((element) => {
    const label = element.closest("label");
    if (!label || label.dataset.helpAttached === "true") {
      return;
    }
    const key = element.getAttribute("name") || label.dataset.helpKey;
    const helpText = FIELD_HELP[key];
    if (!helpText) {
      return;
    }

    const button = document.createElement("button");
    button.type = "button";
    button.className = "help-dot";
    button.textContent = "?";
    button.dataset.helpText = helpText;
    button.addEventListener("pointerenter", () => showFloatingHelpTooltip(button, helpText));
    button.addEventListener("pointerleave", hideFloatingHelpTooltip);
    button.addEventListener("focus", () => showFloatingHelpTooltip(button, helpText));
    button.addEventListener("blur", hideFloatingHelpTooltip);

    if (label.classList.contains("checkbox-field")) {
      label.appendChild(button);
    } else {
      const textSpan = label.querySelector("span");
      if (textSpan && textSpan.parentElement === label) {
        const head = document.createElement("div");
        head.className = "label-head";
        label.insertBefore(head, textSpan);
        head.appendChild(textSpan);
        head.appendChild(button);
      }
    }

    label.dataset.helpAttached = "true";
  });
}

function buildPreviewConfig(flat) {
  const runtimeSecretProvided = form.elements.qq_imap_auth_code.value.trim().length > 0;
  const resolvedQuery = resolveDatasetQuery(flat);
  const metadataOnly = Boolean(flat.metadata_only);
  const workspaceRoot = effectiveWorkspaceRoot(flat.workspace_root);
  return {
    profile_name: activeProfileName,
    ui_persistence: {
      saved_in_browser: true,
      secret_fields_persisted: false,
    },
    event_search: {
      dataset: flat.event_dataset,
      query: resolvedQuery,
      limit_events: optionalLimitValue(flat.limit_events),
      selected_event_tokens: selectedEventTokens(flat),
      selected_regions: selectedRegions(flat),
      metadata_only: metadataOnly,
    },
    station_filter: {
      network_patterns: csvList(flat.network_patterns),
      station_patterns: csvList(flat.station_patterns),
      channel_patterns: csvList(flat.channel_patterns),
      location_priority: csvList(flat.location_priority),
      min_distance_deg: numberValue(flat.min_distance_deg),
      max_distance_deg: numberValue(flat.max_distance_deg),
      min_azimuth_deg: numberValue(flat.min_azimuth_deg),
      max_azimuth_deg: numberValue(flat.max_azimuth_deg),
    },
    request: {
      user: flat.request_user || null,
      email: flat.request_email || null,
      request_label_prefix: flat.request_label_prefix || null,
      window_start_phase: flat.window_start_phase || "P",
      before_arrival_sec: numberValue(flat.before_arrival_sec),
      window_end_phase: flat.window_end_phase || "P",
      after_arrival_sec: numberValue(flat.after_arrival_sec),
      output_format: flat.output_format || "sacbl",
      bundle: flat.bundle || "tar",
      submit_requests: metadataOnly ? false : Boolean(flat.submit_requests),
    },
    mail: {
      qq_imap_user: resolveMailboxEmail(flat) || null,
      poll_interval_sec: numberValue(flat.mail_poll_interval_sec),
      timeout_min: numberValue(flat.mail_timeout_min),
      auth_code: runtimeSecretProvided ? "[provided only in current session]" : "[set at runtime]",
    },
    download: {
      workspace_root: workspaceRoot || null,
    },
    normalize: {
      pre_filt: numericList(flat.pre_filt),
      response_backend: flat.response_backend || null,
      delivery_events_root: workspaceRoot ? `${workspaceRoot}/07_final/events` : null,
      delivery_metadata_root: workspaceRoot ? `${workspaceRoot}/07_final/metadata` : null,
    },
  };
}

function resolveDatasetQuery(flat) {
  const dataset = flat.event_dataset || "custom";
  const preset = datasetPresetValues(dataset);
  const [minLatitude, maxLatitude] = rangePair(flat.latitude_range, [-90, 90]);
  const [minLongitude, maxLongitude] = rangePair(flat.longitude_range, [-180, 180]);
  return {
    starttime: localDateTimeToIso(flat.event_start_utc, preset?.starttime || "1996-01-01T00:00:00"),
    endtime: localDateTimeToIso(flat.event_end_utc, preset?.endtime || "1996-12-31T23:59:59"),
    minmagnitude: numberValue(flat.min_magnitude) ?? preset?.minmagnitude ?? 6.0,
    maxmagnitude: numberValue(flat.max_magnitude) ?? 9.9,
    mindepth: numberValue(flat.min_depth_km) ?? 0,
    maxdepth: numberValue(flat.max_depth_km) ?? 700,
    minlatitude: minLatitude,
    maxlatitude: maxLatitude,
    minlongitude: minLongitude,
    maxlongitude: maxLongitude,
  };
}

function datasetPresetValues(dataset) {
  const now = new Date();
  const end = now.toISOString().slice(0, 19);
  if (dataset === "month0") {
    const start = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    return {
      starttime: start.toISOString().slice(0, 19),
      endtime: end,
      minmagnitude: 0,
    };
  }
  if (dataset === "month") {
    const start = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    return {
      starttime: start.toISOString().slice(0, 19),
      endtime: end,
      minmagnitude: 3,
    };
  }
  if (dataset === "year") {
    const start = new Date(now.getTime() - 366 * 24 * 60 * 60 * 1000);
    return {
      starttime: start.toISOString().slice(0, 19),
      endtime: end,
      minmagnitude: 5,
    };
  }
  if (dataset === "all") {
    return {
      starttime: "1990-01-01T00:00:00",
      endtime: end,
      minmagnitude: 6,
    };
  }
  if (/^\d{4}$/.test(dataset)) {
    return {
      starttime: `${dataset}-01-01T00:00:00`,
      endtime: `${Number(dataset) + 1}-01-01T00:00:00`,
      minmagnitude: 5,
    };
  }
  return null;
}

function buildTomlConfig(flat) {
  const [minLatitude, maxLatitude] = rangePair(flat.latitude_range, [-90, 90]);
  const [minLongitude, maxLongitude] = rangePair(flat.longitude_range, [-180, 180]);
  const selectedEventTokenList = selectedEventTokens(flat);
  const normalizedLimit = tomlLimitValue(flat.limit_events, 20);
  const resolvedQuery = resolveDatasetQuery(flat);
  const metadataOnly = Boolean(flat.metadata_only);

  return [
    "[event_search]",
    `limit = ${normalizedLimit}`,
    `selected_event_tokens = ${tomlArray(selectedEventTokenList)}`,
    "timeout = 30",
    "max_request_attempts = 3",
    "sleep_seconds = 1.0",
    "",
    "[event_search.query]",
    `starttime = ${tomlString(resolvedQuery.starttime || localDateTimeToIso(flat.event_start_utc, "1996-01-01T00:00:00"))}`,
    `endtime = ${tomlString(resolvedQuery.endtime || localDateTimeToIso(flat.event_end_utc, "1996-12-31T23:59:59"))}`,
    `minmagnitude = ${resolvedQuery.minmagnitude ?? numberValue(flat.min_magnitude) ?? 6.0}`,
    `maxmagnitude = ${resolvedQuery.maxmagnitude ?? numberValue(flat.max_magnitude) ?? 9.9}`,
    `mindepth = ${resolvedQuery.mindepth ?? numberValue(flat.min_depth_km) ?? 0}`,
    `maxdepth = ${resolvedQuery.maxdepth ?? numberValue(flat.max_depth_km) ?? 700}`,
    `minlatitude = ${resolvedQuery.minlatitude ?? minLatitude}`,
    `maxlatitude = ${resolvedQuery.maxlatitude ?? maxLatitude}`,
    `minlongitude = ${resolvedQuery.minlongitude ?? minLongitude}`,
    `maxlongitude = ${resolvedQuery.maxlongitude ?? maxLongitude}`,
    'orderby = "time-asc"',
    "limit = 500",
    'output = "text"',
    "",
    "[request]",
    `channels = ${tomlString(flat.channel_patterns || "BH?")}`,
    `networks = ${tomlString(flat.network_patterns || "")}`,
    `stations = ${tomlString(flat.station_patterns || "")}`,
    `location_priority = ${tomlString(flat.location_priority || "00,--,10")}`,
    `min_distance_deg = ${numberValue(flat.min_distance_deg) ?? 35.0}`,
    `max_distance_deg = ${numberValue(flat.max_distance_deg) ?? 95.0}`,
    `min_azimuth_deg = ${numberValue(flat.min_azimuth_deg) ?? -180.0}`,
    `max_azimuth_deg = ${numberValue(flat.max_azimuth_deg) ?? 180.0}`,
    `window_start_before_min = ${secondsToMinutes(flat.before_arrival_sec, 2)}`,
    `window_start_phase = ${tomlString(flat.window_start_phase || "P")}`,
    `window_end_after_min = ${secondsToMinutes(flat.after_arrival_sec, 5)}`,
    `window_end_phase = ${tomlString(flat.window_end_phase || "P")}`,
    `output_format = ${tomlString(flat.output_format || "sacbl")}`,
    `bundle = ${tomlString(flat.bundle || "tar")}`,
    `user = ${tomlString(flat.request_user || "Your Name")}`,
    `email = ${tomlString(flat.request_email || "")}`,
    `request_label_prefix = ${tomlString(flat.request_label_prefix || "wilberflow")}`,
    `submit = ${metadataOnly ? false : Boolean(flat.submit_requests)}`,
    `metadata_only = ${metadataOnly}`,
    "skip_find_stations_prefetch = false",
    "timeout = 30",
    "sleep_seconds = 0.3",
    "max_request_attempts = 5",
    "",
    "[mail]",
    'imap_host = "imap.qq.com"',
    "imap_port = 993",
    "imap_timeout_seconds = 30",
    'imap_user_env = "QQ_IMAP_USER"',
    'imap_password_env = "QQ_IMAP_AUTH_CODE"',
    'mailbox = "INBOX"',
    'subject_substring = "[Success]"',
    'from_substring = "wilber"',
    `poll_interval_seconds = ${asInteger(flat.mail_poll_interval_sec, 30)}`,
    `max_wait_minutes = ${asInteger(flat.mail_timeout_min, 15)}`,
    "message_lookback_hours = 24",
    "max_messages = 1500",
    "prefer_https = true",
    "",
    "[download]",
    "overwrite = false",
    "chunk_size_bytes = 1048576",
    "timeout = 120",
    "",
    "[normalize]",
    `pre_filt = ${tomlString(flat.pre_filt || "0.002,0.005,2.0,4.0")}`,
    'output_unit = "VEL"',
    'routing_type = "earthscope-federator"',
    `response_backend = ${tomlString(flat.response_backend || "local_sac_first")}`,
    "overwrite = false",
    "selected_event_ids = []",
    `limit_events = ${normalizedLimit}`,
    "",
  ].join("\n");
}

function getPatternPickerInput(details) {
  return form.elements[details.dataset.targetInput];
}

function getPatternPickerBoxes(details) {
  return Array.from(details.querySelectorAll('input[type="checkbox"]'));
}

function commitPatternPickerSelection(details) {
  const input = getPatternPickerInput(details);
  const menu = details.querySelector(".pattern-menu");
  if (!input || !menu) {
    return;
  }

  const boxes = Array.from(menu.querySelectorAll('input[type="checkbox"]'));
  const optionValues = new Set(boxes.map((box) => box.value));
  const customTokens = csvList(input.value).filter((token) => !optionValues.has(token));
  const selectedTokens = boxes.filter((box) => box.checked).map((box) => box.value);
  input.value = uniqueList([...selectedTokens, ...customTokens]).join(",");
  syncPatternPicker(details);
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function updatePatternPickerSummary(details, tokens) {
  const summary = details.querySelector(".pattern-summary");
  if (!summary) {
    return;
  }
  if (!tokens.length) {
    summary.textContent = details.dataset.defaultSummary || "从列表选择";
    return;
  }
  const optionValues = new Set(getPatternPickerBoxes(details).map((box) => box.value));
  const customCount = tokens.filter((token) => !optionValues.has(token)).length;
  summary.textContent = customCount > 0 ? `已选 ${tokens.length} 项，含 ${customCount} 个自定义` : `已选 ${tokens.length} 项`;
}

function syncPatternPicker(details) {
  const input = getPatternPickerInput(details);
  if (!input) {
    return;
  }
  const tokens = csvList(input.value);
  const selected = new Set(tokens);
  getPatternPickerBoxes(details).forEach((box) => {
    box.checked = selected.has(box.value);
  });
  updatePatternPickerSummary(details, tokens);
}

function syncAllPatternPickers() {
  document.querySelectorAll(".pattern-picker").forEach((details) => syncPatternPicker(details));
}

function setPatternPickerGroups(details, groups) {
  const menu = details.querySelector(".pattern-menu");
  if (!menu) {
    return;
  }
  menu.innerHTML = "";
  groups.forEach((group) => {
    const section = document.createElement("section");
    section.className = "picker-group";

    const title = document.createElement("h3");
    title.className = "picker-group-title";
    title.textContent = group.label;
    section.appendChild(title);

    const grid = document.createElement("div");
    grid.className = "picker-group-grid";
    (group.options || []).forEach((option) => {
      const label = document.createElement("label");
      label.className = "pick-option";
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = option.value;
      const text = document.createElement("span");
      text.textContent = option.label;
      if (option.title) {
        label.title = option.title;
      }
      label.appendChild(checkbox);
      label.appendChild(text);
      grid.appendChild(label);
    });
    section.appendChild(grid);
    menu.appendChild(section);
  });
  syncPatternPicker(details);
}

function attachPatternPickers() {
  document.querySelectorAll(".pattern-picker").forEach((details) => {
    const input = getPatternPickerInput(details);
    const menu = details.querySelector(".pattern-menu");
    if (!input || details.dataset.pickerAttached === "true") {
      return;
    }

    input.addEventListener("input", () => syncPatternPicker(details));
    menu?.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") {
        return;
      }
      commitPatternPickerSelection(details);
    });

    details.addEventListener("toggle", () => {
      if (details.open) {
        document.querySelectorAll(".pattern-picker[open]").forEach((other) => {
          if (other !== details) {
            other.open = false;
          }
        });
      }
    });

    details.dataset.pickerAttached = "true";
    syncPatternPicker(details);
  });
}

function dispatchFieldInput(element) {
  element.dispatchEvent(new Event("input", { bubbles: true }));
}

function resolveMailboxEmail(flat = collectFlatSettings()) {
  return (flat.request_email || "").trim();
}

function selectedEventTokens(flat = collectFlatSettings()) {
  return csvList(flat.selected_event_tokens);
}

function selectedRegions(flat = collectFlatSettings()) {
  return newlineList(flat.selected_regions);
}

function eventRegion(event) {
  return String(event?.description || "Unknown Region").trim() || "Unknown Region";
}

function currentResultEventIds() {
  return new Set(latestEventResults.map((event) => event.output_event_id).filter(Boolean));
}

function availableRegionValues() {
  return new Set(latestRegionEntries.map((entry) => entry.value));
}

function normalizedSelectedRegions(flat = collectFlatSettings()) {
  const available = availableRegionValues();
  return selectedRegions(flat).filter((region) => available.has(region));
}

function syncSelectedRegionsField({ dispatch = false } = {}) {
  const normalized = normalizedSelectedRegions();
  const nextValue = normalized.join("\n");
  if (form.elements.selected_regions.value !== nextValue) {
    form.elements.selected_regions.value = nextValue;
    if (dispatch) {
      dispatchFieldInput(form.elements.selected_regions);
    }
  }
  return normalized;
}

function getNetworkCatalogValues() {
  return new Set(networkCatalogEntries.map((entry) => entry.value));
}

function computeRegionEntries(events) {
  const counts = new Map();
  (events || []).forEach((event) => {
    const region = eventRegion(event);
    counts.set(region, (counts.get(region) || 0) + 1);
  });
  return Array.from(counts.entries())
    .map(([region, count]) => ({ value: region, count }))
    .sort((left, right) => left.value.localeCompare(right.value, "en"));
}

function filteredRegionEntries() {
  const keyword = (regionFilterSearch?.value || "").trim().toLowerCase();
  if (!keyword) {
    return latestRegionEntries;
  }
  return latestRegionEntries.filter((entry) => entry.value.toLowerCase().includes(keyword));
}

function updateRegionSummary() {
  if (!regionFilterSummary) {
    return;
  }
  if (!latestRegionEntries.length) {
    regionFilterSummary.textContent = "从当前搜索结果地区里筛选";
    return;
  }
  const selected = syncSelectedRegionsField();
  if (selected.length === latestRegionEntries.length) {
    regionFilterSummary.textContent = `已选全部 ${latestRegionEntries.length} 个地区`;
    return;
  }
  if (!selected.length) {
    regionFilterSummary.textContent = "当前未选任何地区";
    return;
  }
  regionFilterSummary.textContent = `已选 ${selected.length} / ${latestRegionEntries.length} 个地区`;
}

function updateRegionStatus(visibleCount = filteredRegionEntries().length) {
  if (!regionFilterStatus) {
    return;
  }
  if (!latestRegionEntries.length) {
    regionFilterStatus.textContent = "搜索 Wilber 事件后会在这里列出所有地区，默认全选";
    return;
  }
  const selected = syncSelectedRegionsField();
  if (!selected.length) {
    regionFilterStatus.textContent = `当前没有选中地区；当前搜索结果共有 ${latestRegionEntries.length} 个地区`;
    return;
  }
  regionFilterStatus.textContent = `当前选中 ${selected.length} 个地区，面板中显示 ${visibleCount} 个地区`;
}

function setSelectedRegions(values, { syncEvents = true } = {}) {
  const available = availableRegionValues();
  const nextValue = uniqueList(values).filter((region) => available.has(region)).join("\n");
  const input = form.elements.selected_regions;
  input.value = nextValue;
  renderRegionFilter();
  if (syncEvents) {
    applyRegionFilterToEventSelection();
  }
  dispatchFieldInput(input);
}

function renderRegionFilter() {
  if (!regionFilterList) {
    return;
  }
  regionFilterList.innerHTML = "";
  if (!latestRegionEntries.length) {
    updateRegionSummary();
    updateRegionStatus(0);
    return;
  }

  const selected = new Set(syncSelectedRegionsField());
  const filtered = filteredRegionEntries();
  if (!filtered.length) {
    regionFilterList.innerHTML = '<div class="empty-state">当前搜索词没有匹配到地区名称。</div>';
    updateRegionSummary();
    updateRegionStatus(0);
    return;
  }

  filtered.forEach((entry) => {
    const label = document.createElement("label");
    label.className = "region-option";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = entry.value;
    checkbox.checked = selected.has(entry.value);
    checkbox.addEventListener("change", () => {
      const next = new Set(selectedRegions());
      if (checkbox.checked) {
        next.add(entry.value);
      } else {
        next.delete(entry.value);
      }
      setSelectedRegions(Array.from(next));
    });

    const body = document.createElement("div");
    body.className = "region-option-body";

    const head = document.createElement("div");
    head.className = "region-option-head";

    const name = document.createElement("strong");
    name.textContent = entry.value;

    const count = document.createElement("span");
    count.className = "region-option-meta";
    count.textContent = `${entry.count} 个事件`;

    head.appendChild(name);
    head.appendChild(count);
    body.appendChild(head);
    label.appendChild(checkbox);
    label.appendChild(body);
    regionFilterList.appendChild(label);
  });

  updateRegionSummary();
  updateRegionStatus(filtered.length);
}

function applyRegionFilterToEventSelection() {
  const selectedRegionSet = new Set(syncSelectedRegionsField());
  const currentIds = currentResultEventIds();
  const manualExtras = selectedEventTokens().filter((token) => !currentIds.has(token));
  const keptResultIds = latestEventResults
    .filter((event) => selectedRegionSet.has(eventRegion(event)))
    .map((event) => event.output_event_id)
    .filter(Boolean);
  form.elements.selected_event_tokens.value = uniqueList([...keptResultIds, ...manualExtras]).join("\n");
  syncEventResultsFromTextarea();
}

function selectedNetworkTokens() {
  return csvList(form.elements.network_patterns.value);
}

function filteredNetworkCatalogEntries() {
  const keyword = (networkCatalogSearch?.value || "").trim().toLowerCase();
  if (!keyword) {
    return networkCatalogEntries;
  }
  return networkCatalogEntries.filter((entry) =>
    `${entry.value} ${entry.name || ""} ${entry.kind || ""}`.toLowerCase().includes(keyword),
  );
}

function updateNetworkCatalogStatus(visibleCount = filteredNetworkCatalogEntries().length) {
  if (!networkCatalogStatus) {
    return;
  }
  if (!networkCatalogLoaded) {
    networkCatalogStatus.textContent = "正在准备常用台网目录...";
    return;
  }
  if (!networkCatalogEntries.length) {
    networkCatalogStatus.textContent = "常用台网目录不可用，仍可手动填写 network_patterns";
    return;
  }

  const catalogValues = getNetworkCatalogValues();
  const tokens = selectedNetworkTokens();
  const selectedCatalogCount = tokens.filter((token) => catalogValues.has(token)).length;
  const customCount = Math.max(0, tokens.length - selectedCatalogCount);

  if (!tokens.length) {
    networkCatalogStatus.textContent = `已载入 ${networkCatalogEntries.length} 个常用台网；当前留空表示每个事件都使用该事件自身的全部可用台网`;
    return;
  }

  const parts = [`已选 ${selectedCatalogCount} 个目录台网`];
  if (customCount > 0) {
    parts.push(`另含 ${customCount} 个手动输入项`);
  }
  parts.push(`当前显示 ${visibleCount} 个匹配项`);
  networkCatalogStatus.textContent = parts.join("，");
}

function updateSelectedEventsStatus() {
  if (!selectedEventsStatus) {
    return;
  }
  const tokens = selectedEventTokens();
  if (!tokens.length) {
    selectedEventsStatus.textContent = latestEventResults.length
      ? "当前没有选中事件；勾选搜索结果或直接编辑上方事件名单"
      : "搜索后会默认全选结果，并同步写入事件名单";
    return;
  }

  const currentResultIds = new Set(latestEventResults.map((event) => event.output_event_id));
  const inCurrentResults = tokens.filter((token) => currentResultIds.has(token)).length;
  const extraManual = tokens.length - inCurrentResults;
  const parts = [`当前事件名单 ${tokens.length} 个`];
  if (latestEventResults.length) {
    parts.push(`当前搜索结果中命中 ${inCurrentResults} 个`);
  }
  if (extraManual > 0) {
    parts.push(`另含 ${extraManual} 个手动补充项`);
  }
  parts.push("搜索结果默认全选，可取消勾选或直接编辑文本框");
  selectedEventsStatus.textContent = parts.join("，");
}

function updateNetworkCatalogSummary() {
  if (!networkCatalogSummary) {
    return;
  }
  const catalogValues = getNetworkCatalogValues();
  const tokens = selectedNetworkTokens();
  const selectedCatalogCount = tokens.filter((token) => catalogValues.has(token)).length;
  const customCount = Math.max(0, tokens.length - selectedCatalogCount);

  if (!tokens.length) {
    networkCatalogSummary.textContent = "从常用台网目录选择";
    return;
  }

  if (customCount > 0) {
    networkCatalogSummary.textContent = `已选 ${selectedCatalogCount} 个目录台网，另含 ${customCount} 个手动项`;
    return;
  }

  networkCatalogSummary.textContent = `已选 ${selectedCatalogCount} 个常用台网`;
}

function setNetworkPatterns(tokens) {
  const nextValue = uniqueList(tokens).join(",");
  const input = form.elements.network_patterns;
  if (input.value === nextValue) {
    syncNetworkCatalogFromInput();
    return;
  }
  input.value = nextValue;
  syncNetworkCatalogFromInput();
  dispatchFieldInput(input);
}

function renderNetworkCatalog() {
  if (!networkCatalogList) {
    return;
  }
  networkCatalogList.innerHTML = "";

  if (!networkCatalogLoaded) {
    updateNetworkCatalogStatus(0);
    return;
  }
  if (!networkCatalogEntries.length) {
    networkCatalogList.innerHTML = '<div class="empty-state">常用台网目录暂时不可用，你仍然可以在上面的输入框手动填写通配符。</div>';
    updateNetworkCatalogStatus(0);
    return;
  }

  const filteredEntries = filteredNetworkCatalogEntries();
  const selected = new Set(selectedNetworkTokens());

  if (!filteredEntries.length) {
    networkCatalogList.innerHTML = '<div class="empty-state">当前搜索词没有匹配到台网代码或名称。</div>';
    updateNetworkCatalogStatus(0);
    return;
  }

  filteredEntries.forEach((entry) => {
    const label = document.createElement("label");
    label.className = "network-option";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selected.has(entry.value);
    checkbox.addEventListener("change", () => {
      const next = new Set(selectedNetworkTokens());
      if (checkbox.checked) {
        next.add(entry.value);
      } else {
        next.delete(entry.value);
      }
      setNetworkPatterns(Array.from(next));
    });

    const body = document.createElement("div");
    body.className = "network-option-body";

    const head = document.createElement("div");
    head.className = "network-option-head";

    const code = document.createElement("strong");
    code.className = "network-option-code";
    code.textContent = entry.value;

    const meta = document.createElement("span");
    meta.className = "network-option-meta";
    meta.textContent = entry.kind === "virtual" ? "虚拟台网" : "实体台网";

    head.appendChild(code);
    head.appendChild(meta);

    const name = document.createElement("div");
    name.className = "network-option-name";
    name.textContent = entry.name || entry.label || entry.value;

    body.appendChild(head);
    body.appendChild(name);
    label.appendChild(checkbox);
    label.appendChild(body);
    networkCatalogList.appendChild(label);
  });

  updateNetworkCatalogStatus(filteredEntries.length);
}

function syncNetworkCatalogFromInput() {
  if (!networkCatalogList) {
    return;
  }
  updateNetworkCatalogSummary();
  renderNetworkCatalog();
}

function selectVisibleNetworkCatalogEntries() {
  const visibleValues = filteredNetworkCatalogEntries().map((entry) => entry.value);
  const tokens = uniqueList([...selectedNetworkTokens(), ...visibleValues]);
  setNetworkPatterns(tokens);
}

function renderProfileSelect(store) {
  const profiles = listProfiles(store);
  profileSelect.innerHTML = profiles.map((name) => `<option value="${name}">${name}</option>`).join("");
  profileSelect.value = activeProfileName;
  profileNameInput.value = activeProfileName;
}

function setStatus(message, persisted = true) {
  const timestamp = new Date().toLocaleTimeString("zh-CN", { hour12: false });
  storageStatus.textContent = `${message} · ${timestamp}`;
  storageStatus.dataset.mode = persisted ? "persisted" : "memory";
}

function refreshUi() {
  syncAllPatternPickers();
  syncNetworkCatalogFromInput();
  syncEventResultsFromTextarea();
  refreshBatchStatus();
  if (form.elements.submit_requests) {
    form.elements.submit_requests.disabled = Boolean(form.elements.metadata_only?.checked);
  }
}

function loadProfile(name, options = {}) {
  const { announce = true } = options;
  const store = loadStore();
  const fallbackName = store.profiles[name] ? name : DEFAULT_PROFILE;
  activeProfileName = fallbackName;
  applySettings(store.profiles[fallbackName] || DEFAULT_SETTINGS);
  clearAllFieldReminders();
  applyWorkspaceRootDefault();
  form.elements.qq_imap_auth_code.value = "";
  saveLastProfileName(activeProfileName);
  renderProfileSelect(store);
  refreshUi();
  if (announce) {
    setStatus(`已载入配置档 ${activeProfileName}`, safeLocalStorageGet(STORAGE_KEY) !== null);
  }
}

function saveCurrentProfile(options = {}) {
  const { announce = true } = options;
  const store = loadStore();
  store.profiles[activeProfileName] = collectFlatSettings();
  const persisted = saveStore(store);
  renderProfileSelect(store);
  refreshUi();
  if (announce) {
    setStatus(`已保存配置档 ${activeProfileName}`, persisted);
  }
}

function saveAsNamedProfile() {
  const requestedName = profileNameInput.value.trim() || activeProfileName || DEFAULT_PROFILE;
  activeProfileName = requestedName;
  saveLastProfileName(activeProfileName);
  saveCurrentProfile();
}

function resetActiveProfile() {
  applySettings(DEFAULT_SETTINGS);
  clearAllFieldReminders();
  applyWorkspaceRootDefault();
  form.elements.qq_imap_auth_code.value = "";
  saveCurrentProfile();
  setStatus(`已恢复 ${activeProfileName} 为默认参数`, safeLocalStorageGet(STORAGE_KEY) !== null);
}

function deleteActiveProfile() {
  const store = loadStore();
  if (activeProfileName === DEFAULT_PROFILE) {
    resetActiveProfile();
    return;
  }
  delete store.profiles[activeProfileName];
  if (!store.profiles[DEFAULT_PROFILE]) {
    store.profiles[DEFAULT_PROFILE] = clone(DEFAULT_SETTINGS);
  }
  const persisted = saveStore(store);
  activeProfileName = DEFAULT_PROFILE;
  saveLastProfileName(activeProfileName);
  loadProfile(activeProfileName, { announce: false });
  setStatus("已删除当前配置档，并切回 Default", persisted);
}

function downloadTextFile(filename, text, type) {
  const blob = new Blob([text], { type });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
}

function exportCurrentJson() {
  const payload = buildPreviewConfig(collectFlatSettings());
  downloadTextFile(`wilberflow_${sanitizeName(activeProfileName)}.json`, `${JSON.stringify(payload, null, 2)}\n`, "application/json");
  setStatus(`已导出 ${activeProfileName} 的 JSON`, safeLocalStorageGet(STORAGE_KEY) !== null);
}

function exportCurrentToml() {
  downloadTextFile(`wilberflow_${sanitizeName(activeProfileName)}.toml`, `${buildTomlConfig(collectFlatSettings())}\n`, "application/toml");
  setStatus(`已导出 ${activeProfileName} 的 config.toml`, safeLocalStorageGet(STORAGE_KEY) !== null);
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || `HTTP ${response.status}`);
  }
  return data;
}

function setWorkflowStatus(message) {
  if (workflowStatus) {
    workflowStatus.textContent = message;
    const text = String(message || "");
    let tone = "neutral";
    if (/失败|错误|缺少|missing/i.test(text)) {
      tone = "error";
    } else if (
      /请先|请选择|请填写|继续已有批次|补跑旧批次前|授权码|邮箱|批次号|Output Format|SAC 输出格式/.test(text)
    ) {
      tone = "warning";
    } else if (/正在|等待启动|运行中|补跑流程已启动|流程已启动/.test(text)) {
      tone = "running";
    } else if (/已完成|补跑已完成/.test(text)) {
      tone = "success";
    }
    workflowStatus.dataset.tone = tone;
  }
}

function focusField(field) {
  if (!field) {
    return;
  }
  field.focus();
  if (typeof field.select === "function" && (field.tagName === "INPUT" || field.tagName === "TEXTAREA")) {
    field.select();
  }
}

function showValidationMessage(name, message, field = form?.elements?.[name]) {
  setWorkflowStatus(message);
  setFieldReminder(name, message);
  if (name === "batch_id" && effectiveBatchMode(form?.elements?.batch_mode?.value) === "existing" && batchIdSelect) {
    focusField(batchIdSelect);
    return;
  }
  focusField(field);
}

function workflowStageSequence(state = {}) {
  const mode = state.mode || "run_all";
  const sequence = Array.isArray(state.stage_sequence) && state.stage_sequence.length
    ? state.stage_sequence
    : (FALLBACK_STAGE_SEQUENCES[mode] || FALLBACK_STAGE_SEQUENCES.run_all);
  return sequence.map((stage) => ({
    key: stage.key,
    label: stage.label || stage.key,
  }));
}

function plannedStageSequenceForFlat(flat, mode = "run_all") {
  if (mode === "resume_from_mail") {
    return workflowStageSequence({ mode: "resume_from_mail" });
  }
  const sequence = [
    { key: "events", label: "搜索事件" },
    { key: "stations", label: "筛选台站" },
  ];
  if (!flat.metadata_only) {
    sequence.push({ key: "requests", label: "生成请求" });
    if (flat.submit_requests) {
      sequence.push(
        { key: "mail", label: "检查邮件" },
        { key: "download", label: "下载数据" },
        { key: "extract", label: "解压数据" },
        { key: "response", label: "去仪器响应" },
        { key: "deliver", label: "整理交付" },
      );
    }
  }
  return sequence;
}

function workflowStageIndex(sequence, stageKey) {
  if (!stageKey) {
    return -1;
  }
  return sequence.findIndex((stage) => stage.key === stageKey);
}

function workflowStageState(overallStatus, currentIndex, index) {
  if (overallStatus === "completed") {
    return "completed";
  }
  if (overallStatus === "failed") {
    if (currentIndex < 0) {
      return "pending";
    }
    if (index < currentIndex) {
      return "completed";
    }
    if (index === currentIndex) {
      return "failed";
    }
    return "pending";
  }
  if (overallStatus === "running") {
    if (currentIndex < 0) {
      return "pending";
    }
    if (index < currentIndex) {
      return "completed";
    }
    if (index === currentIndex) {
      return "active";
    }
    return "pending";
  }
  if (overallStatus === "queued") {
    return index === 0 ? "active" : "pending";
  }
  return "pending";
}

function stageProgressMap(state = {}) {
  const raw = state.stage_progress;
  return raw && typeof raw === "object" ? raw : {};
}

function stageProgressEntry(state = {}, stageKey = "") {
  const progress = stageProgressMap(state)[stageKey];
  return progress && typeof progress === "object" ? progress : {};
}

function stageProgressStats(progress = {}) {
  const raw = progress.stats;
  return raw && typeof raw === "object" ? raw : {};
}

function stageProgressStatItems(stageKey, progress = {}) {
  const stats = stageProgressStats(progress);
  const preferredKeys = stageKey === "stations"
    ? ["succeeded", "failed", "reused", "cache_hits", "fetched", "retry"]
    : stageKey === "requests"
      ? ["prepared", "submitted", "failed", "reused", "retry"]
      : [];
  const labels = {
    succeeded: "成功",
    failed: "失败",
    reused: "复用",
    cache_hits: "缓存",
    fetched: "新抓取",
    retry: "重试",
    prepared: "已准备",
    submitted: "已提交",
  };
  return preferredKeys
    .map((key) => {
      const value = Number(stats[key]);
      if (!Number.isFinite(value) || value <= 0) {
        return null;
      }
      return { key, label: labels[key] || key, value };
    })
    .filter(Boolean);
}

function stageProgressPercent(progress = {}, stageState = "pending") {
  const current = Number(progress.current);
  const total = Number(progress.total);
  if (Number.isFinite(total) && total > 0) {
    const boundedCurrent = Number.isFinite(current) ? Math.max(0, Math.min(current, total)) : 0;
    return Math.max(0, Math.min(100, (boundedCurrent / total) * 100));
  }
  if (stageState === "completed") {
    return 100;
  }
  if (stageState === "failed") {
    return 100;
  }
  return 0;
}

function stageProgressText(stageKey, progress = {}, stageState = "pending") {
  const current = Number(progress.current);
  const total = Number(progress.total);
  const note = String(progress.note || "").trim();
  const parts = [];
  if (Number.isFinite(total) && total > 0) {
    const boundedCurrent = Number.isFinite(current) ? Math.max(0, Math.min(current, total)) : 0;
    parts.push(`${boundedCurrent}/${total}`);
  } else if (stageState === "active") {
    parts.push("进行中");
  } else if (stageState === "completed") {
    parts.push("已完成");
  } else if (stageState === "failed") {
    parts.push("已中断");
  }
  if (note) {
    parts.push(note);
  } else if (stageKey === "mail" && parts.length === 1 && parts[0] !== "进行中") {
    parts.push("等待邮件匹配");
  }
  return parts.join(" · ");
}

function renderWorkflowStagebar(state = {}) {
  if (!workflowStagebar) {
    return;
  }
  const sequence = workflowStageSequence(state);
  if (!sequence.length) {
    workflowStagebar.innerHTML = '<div class="empty-state">等待任务启动后显示阶段进度。</div>';
    return;
  }
  const currentIndex = workflowStageIndex(sequence, state.current_stage_key || "");
  const overallStatus = state.status || "idle";
  const mailExpectedCount = Number(state.mail_expected_count || 0);
  const mailReceivedCount = Number(state.mail_received_count || 0);
  const mailPendingCount = Number(state.mail_pending_count || 0);
  workflowStagebar.innerHTML = sequence
    .map((stage, index) => {
      const stageState = workflowStageState(overallStatus, currentIndex, index);
      const progress = stageProgressEntry(state, stage.key);
      const stateText = stageState === "completed"
        ? "已完成"
        : stageState === "active"
          ? (overallStatus === "queued" ? "等待启动" : "当前阶段")
          : stageState === "failed"
            ? "执行失败"
            : "待执行";
      let metaText = stageProgressText(stage.key, progress, stageState);
      const statItems = stageProgressStatItems(stage.key, progress);
      if (stage.key === "mail" && mailExpectedCount > 0) {
        metaText = [`${mailReceivedCount}/${mailExpectedCount}`, `剩余 ${mailPendingCount}`]
          .concat(String(progress.note || "").trim() ? [String(progress.note || "").trim()] : [])
          .join(" · ");
      }
      const stepNumber = String(index + 1).padStart(2, "0");
      const progressPercent = stageProgressPercent(progress, stageState);
      const progressStatus = String(progress.status || "");
      const progressTone = stageState === "failed" || progressStatus === "failed"
        ? "failed"
        : stageState === "completed" || progressStatus === "completed"
          ? "completed"
          : stageState === "active" || progressStatus === "running"
            ? "running"
            : "pending";
      const indeterminate = progressPercent === 0 && progressTone === "running";
      return `
        <div class="workflow-stage is-${stageState}">
          <div class="workflow-stage-dot">${stepNumber}</div>
          <div class="workflow-stage-body">
            <div class="workflow-stage-top">
              <div class="workflow-stage-index">Step ${stepNumber}</div>
              <div class="workflow-stage-state is-${stageState}">${stateText}</div>
            </div>
            <div class="workflow-stage-title">${stage.label}</div>
            <div class="workflow-stage-progress is-${progressTone}">
              <div class="workflow-stage-progress-fill ${indeterminate ? "is-indeterminate" : ""}" style="width: ${indeterminate ? 42 : progressPercent}%;"></div>
            </div>
            ${statItems.length ? `<div class="workflow-stage-stats">${statItems.map((item) => `<span class="workflow-stage-stat is-${item.key}">${item.label} ${item.value}</span>`).join("")}</div>` : ""}
            ${metaText ? `<div class="workflow-stage-meta">${metaText}</div>` : ""}
          </div>
        </div>
      `;
    })
    .join("");
}

function formatWorkflowStatus(state = {}) {
  const message = state.message || "等待运行";
  const sequence = workflowStageSequence(state);
  const currentIndex = workflowStageIndex(sequence, state.current_stage_key || "");
  const batchId = String(state.batch_id || "").trim();
  const mailExpectedCount = Number(state.mail_expected_count || 0);
  const mailReceivedCount = Number(state.mail_received_count || 0);
  const mailPendingCount = Number(state.mail_pending_count || 0);
  const currentStageProgress = stageProgressEntry(state, state.current_stage_key || "");
  const stageText = sequence.length
    ? currentIndex >= 0
      ? `第 ${Math.min(currentIndex + 1, sequence.length)}/${sequence.length} 阶段`
      : `共 ${sequence.length} 阶段`
    : "";
  const mailText = mailExpectedCount > 0 ? `邮件 ${mailReceivedCount}/${mailExpectedCount}，剩余 ${mailPendingCount}` : "";
  const progressText = stageProgressText(state.current_stage_key || "", currentStageProgress, state.status === "failed" ? "failed" : "active");
  const progressStatsText = stageProgressStatItems(state.current_stage_key || "", currentStageProgress)
    .map((item) => `${item.label} ${item.value}`)
    .join(" · ");
  const batchText = batchId ? `批次 ${batchId}` : "";
  const workspace = state.workspace_root ? ` · ${state.workspace_root}` : "";
  return [message, batchText, stageText, mailText, progressText, progressStatsText].filter(Boolean).join(" · ") + workspace;
}

async function pollWorkflowStatus() {
  try {
    const payload = await fetchJson("/api/workflow/status");
    const state = payload.state || {};
    const status = state.status || "idle";
    setWorkflowStatus(formatWorkflowStatus(state));
    renderWorkflowStagebar(state);
    if (runWorkflowButton) {
      runWorkflowButton.disabled = status === "queued" || status === "running";
    }
    if (resumeMailWorkflowButton) {
      resumeMailWorkflowButton.disabled = status === "queued" || status === "running";
    }
    if (status === "queued" || status === "running") {
      if (!workflowStatusTimer) {
        workflowStatusTimer = window.setTimeout(() => {
          workflowStatusTimer = null;
          pollWorkflowStatus();
        }, 3000);
      }
    } else if (workflowStatusTimer) {
      window.clearTimeout(workflowStatusTimer);
      workflowStatusTimer = null;
    }
  } catch (error) {
    setWorkflowStatus(`运行状态获取失败: ${String(error.message || error)}`);
    renderWorkflowStagebar({ status: "idle", message: "运行状态获取失败" });
  }
}

async function runWorkflow() {
  const flat = collectFlatSettings({ includeSensitive: true });
  clearAllFieldReminders();
  const workspaceRoot = effectiveWorkspaceRoot(flat.workspace_root);
  const batchMode = effectiveBatchMode(flat.batch_mode);
  const batchId = effectiveBatchId(flat.batch_id);
  const willSubmitRequests = !flat.metadata_only && Boolean(flat.submit_requests);
  const sacOutputFormats = new Set(["sacbl", "sacbb", "saca"]);
  if (!workspaceRoot) {
    showValidationMessage("workspace_root", "请先填写工作根目录");
    return;
  }
  if (batchMode === "existing" && !batchId) {
    showValidationMessage("batch_id", "继续已有批次时，请先从下拉里选择一个批次，或在右侧手动输入批次号");
    return;
  }
  if (willSubmitRequests && !sacOutputFormats.has(flat.output_format || "")) {
    showValidationMessage("output_format", "完整下载与去响应流程目前只支持 SAC 输出格式，请将 Output Format 设为 sacbl、sacbb 或 saca");
    return;
  }
  if (willSubmitRequests && !flat.request_email) {
    showValidationMessage("request_email", "请先填写接收邮箱 / QQ邮箱，Wilber 成功邮件和本地收信都会用到它");
    return;
  }
  if (willSubmitRequests && !flat.qq_imap_auth_code) {
    showValidationMessage("qq_imap_auth_code", "直接提交请求时，请填写 QQ 授权码，否则脚本无法自动检查 [Success] 邮件");
    return;
  }

  const configToml = buildTomlConfig(flat);
  setWorkflowStatus("正在提交本地运行任务...");
  renderWorkflowStagebar({
    status: "queued",
    mode: "run_all",
    stage_sequence: plannedStageSequenceForFlat(flat, "run_all"),
  });
  if (runWorkflowButton) {
    runWorkflowButton.disabled = true;
  }
  if (resumeMailWorkflowButton) {
    resumeMailWorkflowButton.disabled = true;
  }
  try {
    const payload = await postJson("/api/workflow/run", {
      workspace_root: workspaceRoot,
      batch_mode: batchMode,
      batch_id: batchId,
      request_email: flat.request_email,
      qq_imap_auth_code: flat.qq_imap_auth_code,
      config_toml: configToml,
    });
    setWorkflowStatus([payload.message || "流程已启动", payload.batch_id ? `批次 ${payload.batch_id}` : ""].filter(Boolean).join(" · "));
    if (batchMode === "existing" && payload.batch_id && form.elements.batch_id) {
      form.elements.batch_id.value = payload.batch_id;
    }
    await loadBatchCatalog({ announceError: false });
    await pollWorkflowStatus();
  } catch (error) {
    setWorkflowStatus(`启动失败: ${String(error.message || error)}`);
    if (runWorkflowButton) {
      runWorkflowButton.disabled = false;
    }
    if (resumeMailWorkflowButton) {
      resumeMailWorkflowButton.disabled = false;
    }
  }
}

async function resumeMailWorkflow() {
  const flat = collectFlatSettings({ includeSensitive: true });
  clearAllFieldReminders();
  const workspaceRoot = effectiveWorkspaceRoot(flat.workspace_root);
  const batchMode = effectiveBatchMode(flat.batch_mode);
  const batchId = effectiveBatchId(flat.batch_id);
  if (!workspaceRoot) {
    showValidationMessage("workspace_root", "请先填写工作根目录");
    return;
  }
  if (batchMode !== "existing") {
    showValidationMessage("batch_mode", "补跑旧批次前，请先把“批次处理”切换成“继续已有批次”");
    return;
  }
  if (!batchId) {
    showValidationMessage("batch_id", "补跑旧批次前，请先从下拉里选择一个已有批次，或手动输入批次号");
    return;
  }
  if (!flat.request_email) {
    showValidationMessage("request_email", "补跑收信与下载时，请填写接收邮箱 / QQ邮箱");
    return;
  }
  if (!flat.qq_imap_auth_code) {
    showValidationMessage("qq_imap_auth_code", "补跑收信与下载时，请填写 QQ 授权码，脚本才可以重新检查 [Success] 邮件");
    return;
  }

  setWorkflowStatus("正在提交补跑任务...");
  renderWorkflowStagebar({
    status: "queued",
    mode: "resume_from_mail",
    stage_sequence: plannedStageSequenceForFlat(flat, "resume_from_mail"),
  });
  if (runWorkflowButton) {
    runWorkflowButton.disabled = true;
  }
  if (resumeMailWorkflowButton) {
    resumeMailWorkflowButton.disabled = true;
  }
  try {
    const payload = await postJson("/api/workflow/resume-mail", {
      workspace_root: workspaceRoot,
      batch_mode: batchMode,
      batch_id: batchId,
      request_email: flat.request_email,
      qq_imap_auth_code: flat.qq_imap_auth_code,
    });
    setWorkflowStatus([payload.message || "补跑流程已启动", payload.batch_id ? `批次 ${payload.batch_id}` : ""].filter(Boolean).join(" · "));
    if (payload.batch_id && form.elements.batch_id) {
      form.elements.batch_id.value = payload.batch_id;
    }
    await pollWorkflowStatus();
  } catch (error) {
    setWorkflowStatus(`补跑启动失败: ${String(error.message || error)}`);
    if (runWorkflowButton) {
      runWorkflowButton.disabled = false;
    }
    if (resumeMailWorkflowButton) {
      resumeMailWorkflowButton.disabled = false;
    }
  }
}

function renderSelectOptions(select, values, { multiplyBy = 1, suffix = "" } = {}) {
  select.innerHTML = values
    .map((value) => {
      const actual = value * multiplyBy;
      const label = suffix ? `${value} ${suffix}` : String(actual);
      return `<option value="${actual}">${label}</option>`;
    })
    .join("");
}

function renderLabeledOptions(select, options) {
  select.innerHTML = (options || [])
    .map((option) => `<option value="${option.value}">${option.label}</option>`)
    .join("");
}

function renderEventDatasetOptions(options) {
  eventDatasetSelect.innerHTML = options.map((option) => `<option value="${option.value}">${option.label}</option>`).join("");
}

function renderEventResultsList(events) {
  latestEventResults = events;
  latestRegionEntries = computeRegionEntries(events);
  if (!events.length) {
    eventResults.innerHTML = '<div class="empty-state">没有返回事件，请缩小时间范围或放宽筛选条件。</div>';
    renderRegionFilter();
    updateSelectedEventsStatus();
    return;
  }
  const selected = new Set(selectedEventTokens());
  eventResults.innerHTML = events
    .map(
      (event) => `
        <label class="event-card" data-region="${eventRegion(event).replace(/"/g, "&quot;")}">
          <input type="checkbox" class="event-result-checkbox" data-output-id="${event.output_event_id}" data-event-time="${event.event_time_utc}" data-region="${eventRegion(event).replace(/"/g, "&quot;")}" ${selected.has(event.output_event_id) ? "checked" : ""} />
          <div class="event-card-body">
            <div class="event-card-top">
              <strong>${event.output_event_id}</strong>
              <span>${event.magnitude_type || "M"} ${event.magnitude ?? "?"}</span>
            </div>
            <div class="event-card-meta">${event.event_time_utc} · 深度 ${event.depth_km ?? "?"} km</div>
            <div class="event-card-desc">${event.description || "No description"}</div>
          </div>
        </label>
      `,
    )
    .join("");
  renderRegionFilter();
}

function syncSelectedEventsToTextarea() {
  const checked = Array.from(document.querySelectorAll(".event-result-checkbox:checked"));
  const resultIds = currentResultEventIds();
  const manualExtras = selectedEventTokens().filter((token) => !resultIds.has(token));
  const tokens = checked.map((checkbox) => checkbox.dataset.outputId || "").filter(Boolean);
  form.elements.selected_event_tokens.value = uniqueList([...tokens, ...manualExtras]).join("\n");
  refreshUi();
  dispatchFieldInput(form.elements.selected_event_tokens);
}

function syncEventResultsFromTextarea() {
  const selectedRegionSet = new Set(syncSelectedRegionsField());
  const selected = new Set(selectedEventTokens());
  document.querySelectorAll(".event-result-checkbox").forEach((checkbox) => {
    if (!(checkbox instanceof HTMLInputElement)) {
      return;
    }
    const region = checkbox.dataset.region || "";
    const card = checkbox.closest(".event-card");
    const regionVisible = !latestRegionEntries.length || selectedRegionSet.has(region);
    checkbox.checked = regionVisible && selected.has(checkbox.dataset.outputId || "");
    if (card) {
      card.classList.toggle("is-hidden", !regionVisible);
    }
  });
  renderRegionFilter();
  updateSelectedEventsStatus();
}

async function loadStudioMetadata() {
  studioMetadata = await fetchJson("/api/wilber/ui-metadata");
  const currentSettings = collectFlatSettings();
  renderEventDatasetOptions(studioMetadata.event_datasets || []);
  renderSelectOptions(beforeArrivalSelect, studioMetadata.timewindow_before_options_min || [], { multiplyBy: 60, suffix: "min" });
  renderSelectOptions(afterArrivalSelect, studioMetadata.timewindow_after_options_min || [], { multiplyBy: 60, suffix: "min" });
  renderLabeledOptions(form.elements.output_format, studioMetadata.output_format_options || []);
  renderLabeledOptions(form.elements.bundle, studioMetadata.bundle_options || []);
  eventDatasetSelect.value = currentSettings.event_dataset || DEFAULT_SETTINGS.event_dataset;
  beforeArrivalSelect.value = currentSettings.before_arrival_sec || DEFAULT_SETTINGS.before_arrival_sec;
  afterArrivalSelect.value = currentSettings.after_arrival_sec || DEFAULT_SETTINGS.after_arrival_sec;
  form.elements.output_format.value = currentSettings.output_format || DEFAULT_SETTINGS.output_format;
  form.elements.bundle.value = currentSettings.bundle || DEFAULT_SETTINGS.bundle;
  const channelPicker = document.querySelector('.pattern-picker[data-target-input="channel_patterns"]');
  if (channelPicker) {
    setPatternPickerGroups(channelPicker, studioMetadata.channel_groups || []);
  }
  if (!form.elements.network_patterns.value) {
    form.elements.network_patterns.value = studioMetadata.defaults?.networks || "";
  }
  if (!form.elements.channel_patterns.value) {
    form.elements.channel_patterns.value = studioMetadata.defaults?.channels || "";
  }
  if (!form.elements.event_dataset.value) {
    form.elements.event_dataset.value = DEFAULT_SETTINGS.event_dataset;
  }
  applyWorkspaceRootDefault();
  refreshUi();
}

async function loadNetworkCatalog() {
  if (!networkCatalogStatus) {
    return;
  }
  networkCatalogLoaded = false;
  updateNetworkCatalogStatus(0);
  try {
    const limit = studioMetadata?.network_catalog_limit || 500;
    const payload = await fetchJson(`/api/wilber/network-catalog?limit=${encodeURIComponent(limit)}`);
    networkCatalogEntries = Array.isArray(payload.catalog) ? payload.catalog : [];
    networkCatalogLoaded = true;
    renderNetworkCatalog();
  } catch (error) {
    networkCatalogEntries = [];
    networkCatalogLoaded = true;
    renderNetworkCatalog();
    networkCatalogStatus.textContent = `常用台网目录加载失败: ${String(error.message || error)}`;
  }
}

async function loadBatchCatalog(options = {}) {
  const { announceError = true } = options;
  const baseRoot = effectiveWorkspaceRoot(form?.elements?.workspace_root?.value);
  const datalist = document.getElementById("batch-catalog-options");
  if (!baseRoot) {
    batchCatalogEntries = [];
    if (datalist) {
      datalist.innerHTML = "";
    }
    refreshBatchStatus();
    return;
  }
  if (batchCatalogStatus) {
    batchCatalogStatus.textContent = "正在读取已有批次目录...";
  }
  try {
    const payload = await fetchJson(`/api/workflow/batches?workspace_root=${encodeURIComponent(baseRoot)}`);
    batchCatalogEntries = Array.isArray(payload.batches) ? payload.batches : [];
    if (datalist) {
      datalist.innerHTML = batchCatalogEntries
        .map((entry) => `<option value="${entry.batch_id}">${entry.batch_id}</option>`)
        .join("");
    }
    if (batchIdSelect) {
      const currentValue = effectiveBatchId(form?.elements?.batch_id?.value);
      batchIdSelect.innerHTML = [
        '<option value="">从已有批次下拉选择</option>',
        ...batchCatalogEntries.map((entry) => {
          const status = entry.status ? ` · ${entry.status}` : "";
          return `<option value="${entry.batch_id}">${entry.batch_id}${status}</option>`;
        }),
      ].join("");
      batchIdSelect.value = batchCatalogEntries.some((entry) => entry.batch_id === currentValue) ? currentValue : "";
    }
    refreshBatchStatus();
  } catch (error) {
    batchCatalogEntries = [];
    if (datalist) {
      datalist.innerHTML = "";
    }
    if (batchIdSelect) {
      batchIdSelect.innerHTML = '<option value="">从已有批次下拉选择</option>';
    }
    refreshBatchStatus();
    if (batchCatalogStatus && announceError) {
      batchCatalogStatus.textContent = `读取已有批次失败: ${String(error.message || error)}`;
    }
  }
}

function applyDatasetPreset() {
  const flat = collectFlatSettings();
  const preset = datasetPresetValues(flat.event_dataset || "custom");
  if (!preset) {
    return;
  }
  if (preset.starttime) {
    form.elements.event_start_utc.value = preset.starttime.slice(0, 16);
  }
  if (preset.endtime) {
    form.elements.event_end_utc.value = preset.endtime.slice(0, 16);
  }
  if (preset.minmagnitude !== undefined && preset.minmagnitude !== null) {
    form.elements.min_magnitude.value = String(preset.minmagnitude);
  }
  setStatus(`已套用 ${flat.event_dataset} 的 Wilber 预设范围`, safeLocalStorageGet(STORAGE_KEY) !== null);
}

function buildSearchQueryString(flat) {
  const resolved = resolveDatasetQuery(flat);
  const params = new URLSearchParams();
  params.set("dataset", flat.event_dataset || "custom");
  Object.entries(resolved).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") {
      params.set(key, String(value));
    }
  });
  params.set("limit", String(optionalLimitValue(flat.limit_events) || 200));
  return params.toString();
}

async function searchWilberEvents() {
  const flat = collectFlatSettings();
  eventSearchStatus.textContent = "正在搜索 Wilber 事件...";
  try {
    const payload = await fetchJson(`/api/wilber/search-events?${buildSearchQueryString(flat)}`);
    latestRegionEntries = computeRegionEntries(payload.events || []);
    form.elements.selected_regions.value = latestRegionEntries.map((entry) => entry.value).join("\n");
    const defaultTokens = (payload.events || []).map((event) => event.output_event_id).filter(Boolean);
    form.elements.selected_event_tokens.value = uniqueList(defaultTokens).join("\n");
    renderEventResultsList(payload.events || []);
    syncEventResultsFromTextarea();
    saveCurrentProfile({ announce: false });
    eventSearchStatus.textContent = `已返回 ${payload.count || 0} 个事件`;
  } catch (error) {
    eventResults.innerHTML = `<div class="empty-state">${String(error.message || error)}</div>`;
    eventSearchStatus.textContent = "事件搜索失败";
  }
}

function handleEventResultInteraction(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) || !target.classList.contains("event-result-checkbox")) {
    return;
  }
  syncSelectedEventsToTextarea();
}

function bindEventListeners() {
  form.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target === networkCatalogSearch) {
      renderNetworkCatalog();
      return;
    }
    if (target === regionFilterSearch) {
      renderRegionFilter();
      return;
    }
    if (!target.getAttribute("name")) {
      return;
    }

    clearFieldReminder(target.getAttribute("name"));

    refreshUi();
    saveCurrentProfile({ announce: false });
    setStatus(`自动保存 ${activeProfileName}`, safeLocalStorageGet(STORAGE_KEY) !== null);

    if (target.getAttribute("name") === "event_dataset") {
      applyDatasetPreset();
    }
  });

  form.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement) || !target.getAttribute("name")) {
      return;
    }
    clearFieldReminder(target.getAttribute("name"));
    refreshUi();
    saveCurrentProfile({ announce: false });
    setStatus(`自动保存 ${activeProfileName}`, safeLocalStorageGet(STORAGE_KEY) !== null);
    if (target.getAttribute("name") === "workspace_root" || target.getAttribute("name") === "batch_mode") {
      loadBatchCatalog({ announceError: false });
    }
  });

  profileSelect.addEventListener("change", (event) => {
    loadProfile(event.target.value);
  });

  document.getElementById("save-profile").addEventListener("click", saveAsNamedProfile);
  document.getElementById("reset-profile").addEventListener("click", resetActiveProfile);
  document.getElementById("delete-profile").addEventListener("click", deleteActiveProfile);
  importConfigButton?.addEventListener("click", openImportConfigPicker);
  importConfigFileInput?.addEventListener("change", handleImportConfigFile);
  document.getElementById("export-json").addEventListener("click", exportCurrentJson);
  document.getElementById("export-toml").addEventListener("click", exportCurrentToml);
  batchCatalogRefreshButton?.addEventListener("click", () => loadBatchCatalog({ announceError: true }));
  batchIdSelect?.addEventListener("change", () => {
    const selected = batchIdSelect.value || "";
    if (form.elements.batch_id) {
      form.elements.batch_id.value = selected;
    }
    clearFieldReminder("batch_id");
    refreshUi();
    saveCurrentProfile({ announce: false });
  });
  batchIdInput?.addEventListener("input", () => {
    if (batchIdSelect) {
      const current = batchIdInput.value.trim();
      batchIdSelect.value = batchCatalogEntries.some((entry) => entry.batch_id === current) ? current : "";
    }
  });
  searchEventsButton.addEventListener("click", searchWilberEvents);
  runWorkflowButton?.addEventListener("click", runWorkflow);
  resumeMailWorkflowButton?.addEventListener("click", resumeMailWorkflow);
  networkSelectVisibleButton?.addEventListener("click", selectVisibleNetworkCatalogEntries);
  networkClearSelectionButton?.addEventListener("click", () => setNetworkPatterns([]));
  networkCloseDropdownButton?.addEventListener("click", () => {
    if (networkCatalogDropdown) {
      networkCatalogDropdown.open = false;
    }
  });
  regionSelectVisibleButton?.addEventListener("click", () => setSelectedRegions(filteredRegionEntries().map((entry) => entry.value)));
  regionClearSelectionButton?.addEventListener("click", () => setSelectedRegions([]));
  regionCloseDropdownButton?.addEventListener("click", () => {
    const dropdown = document.getElementById("region-filter-dropdown");
    if (dropdown) {
      dropdown.open = false;
    }
  });
  eventResults.addEventListener("change", handleEventResultInteraction);
}

async function bootstrap() {
  attachHelpTooltips();
  attachPatternPickers();
  window.addEventListener("scroll", hideFloatingHelpTooltip, { passive: true });
  window.addEventListener("resize", hideFloatingHelpTooltip);

  const store = loadStore();
  const lastProfile = loadLastProfileName();
  if (!store.profiles[lastProfile]) {
    store.profiles[DEFAULT_PROFILE] = store.profiles[DEFAULT_PROFILE] || clone(DEFAULT_SETTINGS);
    saveStore(store);
  }

  activeProfileName = store.profiles[lastProfile] ? lastProfile : DEFAULT_PROFILE;
  renderProfileSelect(store);
  applySettings(store.profiles[activeProfileName] || DEFAULT_SETTINGS);
  clearAllFieldReminders();
  applyWorkspaceRootDefault();
  form.elements.qq_imap_auth_code.value = "";
  refreshUi();
  setStatus("已恢复上次浏览器默认配置", safeLocalStorageGet(STORAGE_KEY) !== null);
  renderWorkflowStagebar({ status: "idle", mode: "run_all" });

  bindEventListeners();

  try {
    await loadStudioMetadata();
    await loadBatchCatalog({ announceError: false });
    await loadNetworkCatalog();
    await pollWorkflowStatus();
    eventSearchStatus.textContent = "本地 Wilber 元数据已连接";
  } catch (error) {
    eventSearchStatus.textContent = "未连到本地 server";
    networkCatalogLoaded = true;
    renderNetworkCatalog();
    if (networkCatalogStatus) {
      networkCatalogStatus.textContent = "常用台网目录不可用";
    }
    eventResults.innerHTML = '<div class="empty-state">请先运行 `wilberflow serve`，然后再打开这个页面。</div>';
  }
}

bootstrap();
