import { createEffect, createSignal, For, onCleanup, onMount, Show } from "solid-js";
import { A } from "@solidjs/router";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { Unicode11Addon } from "@xterm/addon-unicode11";
import { SearchAddon } from "@xterm/addon-search";
import { api, invalidateTerminalCsrfToken, terminalCsrfToken, terminalWebSocketUrl } from "../api.js";
import {
  TYPES,
  FLAGS,
  DIRECTIONS,
  INPUT_MAX_PAYLOAD,
  HEARTBEAT_MS,
  encodeFrame,
  decodeFrame,
  encodeJson,
  decodeJson,
  encodeResize,
  encodePing,
  decodePing
} from "../terminal-protocol.js";
import {
  FIT_VERIFY_DELAYS_MS,
  LIVE_WRITE_BATCH_MS,
  joinTerminalPayloads,
  shouldRefitTerminal,
  terminalResizeSettleDelay
} from "../terminal-rendering.js";
import "@xterm/xterm/css/xterm.css";

const encoder = new TextEncoder();
const SESSION_NAME = /^[a-z0-9-]{1,32}$/;
const DEFAULT_FONT_SIZE = 14;
const MOBILE_FONT_SIZE = 12;
const MIN_FONT_SIZE = 11;
const MAX_FONT_SIZE = 20;
const FONT_SIZE_STORAGE_KEY = "reaper:terminal-font-size";
const SESSION_STORAGE_KEY_PREFIX = "reaper:terminal-session:";
const SEARCH_DECORATIONS = Object.freeze({
  matchBackground: "#34343c",
  matchBorder: "#565661",
  matchOverviewRuler: "#686875",
  activeMatchBackground: "#6c7890",
  activeMatchBorder: "#c7c9d1",
  activeMatchColorOverviewRuler: "#c7c9d1"
});
const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled]):not([type='hidden'])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])"
].join(",");

function normalizeSession(record) {
  const source = typeof record === "string" ? { name: record } : record;
  const name = String(source?.name || source?.sessionName || source?.sessionId || "").split("/").pop().trim();
  if (!name) return null;
  return {
    ...source,
    name,
    title: String(source?.title || name),
    state: source?.state || "ready"
  };
}

export function ProjectTerminal(props) {
  let workspaceRef;
  let hostRef;
  let terminalCanvasRef;
  let fullscreenButtonRef;
  let findInputRef;
  let sessionMenuRef;
  let term;
  let fit;
  let search;
  let searchResultDisposable;
  let terminalEventDisposables = [];
  let ws;
  let outsideState = [];
  let documentKeyHandler;
  let documentFocusHandler;
  let documentPointerHandler;
  let disposed = false;
  let helloAcknowledged = false;
  let reconnectTimer;
  let reconnectAttempt = 0;
  let heartbeatTimer;
  let heartbeatMs = HEARTBEAT_MS;
  let missedPongs = 0;
  let fitFrame;
  let resizeOutputTimer;
  let resizeOutputSettling = false;
  let fitVerificationTimers = [];
  let fontLoadingHandler;
  let toolNoticeTimer;
  let fontSizeCustomized = false;
  let requestCounter = 0;
  let controlSequence = 0;
  let activeStreamId = 0;
  let inputEnabled = false;
  let restoreFocus;
  let sessionLoadSequence = 0;
  let refreshCsrfOnReconnect = false;
  let sessionsLoadPromise;
  let transportConnectedOnce = false;
  let terminalResetSequence = 0;
  let selectionSequence = 0;
  let transportAttemptCount = 0;
  let pendingSessionMutations = 0;
  let activeInteractionGeneration = 0;
  let refreshSessionsAfterMutations = false;
  const pendingOpens = new Map();
  const streams = new Map();
  const tabRefs = new Map();
  const userDeletionFocus = new Set();
  const sessionRevisions = new Map();
  const sessionMutationWaiters = [];
  const [hasSelection, setHasSelection] = createSignal(false);
  const [findOpen, setFindOpen] = createSignal(false);
  const [findQuery, setFindQuery] = createSignal("");
  const [findResults, setFindResults] = createSignal({ resultIndex: 0, resultCount: 0 });
  const [fontSize, setFontSize] = createSignal(DEFAULT_FONT_SIZE);
  const [dimensions, setDimensions] = createSignal({ cols: 80, rows: 24 });
  const [renderer] = createSignal("Canvas");
  const [toolNotice, setToolNotice] = createSignal("");
  const [mobileControlsOpen, setMobileControlsOpen] = createSignal(false);
  const [mobileScrollMaximum, setMobileScrollMaximum] = createSignal(0);
  const [mobileScrollPosition, setMobileScrollPosition] = createSignal(0);

  const [sessions, setSessions] = createSignal([]);
  const [selectedName, setSelectedName] = createSignal("");
  const [listState, setListState] = createSignal("loading");
  const [connectionState, setConnectionState] = createSignal("connecting");
  const [statusText, setStatusText] = createSignal("Connecting to terminal…");
  const [roundTripMs, setRoundTripMs] = createSignal(null);
  const [opening, setOpening] = createSignal(false);
  const [expanded, setExpanded] = createSignal(false);
  const [degraded, setDegraded] = createSignal(false);
  const [editingName, setEditingName] = createSignal("");
  const [editingTitle, setEditingTitle] = createSignal("");
  const [busyName, setBusyName] = createSignal("");
  const [sessionError, setSessionError] = createSignal("");
  const [historyBytes, setHistoryBytes] = createSignal(0);
  const [creatingSession, setCreatingSession] = createSignal(false);
  const [newSessionName, setNewSessionName] = createSignal("");

  const projectPath = () => `/api/projects/${encodeURIComponent(props.name)}`;
  const selectedSessionStorageKey = () => `${SESSION_STORAGE_KEY_PREFIX}${encodeURIComponent(props.name)}`;
  const activeSession = () => sessions().find((session) => session.name === selectedName());
  const editingSession = () => sessions().find((session) => session.name === editingName());
  const connected = () => connectionState() === "ready" && inputEnabled;
  const connectionLabel = () => {
    if (connected()) return "Live";
    if (connectionState() === "error") return "Error";
    if (connectionState() === "offline") return "Offline";
    if (connectionState() === "reconnecting") return "Reconnecting";
    if (connectionState() === "loading") return "Restoring";
    return "Connecting";
  };
  const connectionHeading = () => {
    if (listState() === "ready" && sessions().length === 0) return "No terminal sessions";
    if (connectionState() === "error") return "Terminal unavailable";
    if (connectionState() === "offline" || connectionState() === "reconnecting") return "Restoring your session";
    if (opening() && historyBytes() > 0) return "Replaying session history";
    if (opening()) return `Opening ${sessionLabel(activeSession())}`;
    return "Connecting to terminal";
  };
  const findResultLabel = () => {
    if (!findQuery()) return "Type to search";
    const result = findResults();
    if (!result.resultCount) return "No matches";
    if (result.resultIndex < 0) return `${result.resultCount}+ results`;
    return `${result.resultIndex + 1} of ${result.resultCount}`;
  };

  function sessionLabel(session) {
    return session?.title || session?.name || "Terminal";
  }
  function sessionRevision(name) {
    return sessionRevisions.get(name) || 0;
  }
  function bumpSessionRevision(name) {
    const revision = sessionRevision(name) + 1;
    sessionRevisions.set(name, revision);
    return revision;
  }
  function beginSessionMutation() {
    pendingSessionMutations += 1;
    if (sessionsLoadPromise || listState() === "loading") refreshSessionsAfterMutations = true;
    sessionLoadSequence += 1;
    sessionsLoadPromise = undefined;
  }
  function endSessionMutation() {
    pendingSessionMutations = Math.max(0, pendingSessionMutations - 1);
    if (pendingSessionMutations === 0) {
      for (const resolve of sessionMutationWaiters.splice(0)) resolve();
      if (refreshSessionsAfterMutations) {
        refreshSessionsAfterMutations = false;
        queueMicrotask(() => { if (!disposed) void loadSessions(); });
      }
    }
  }
  function waitForSessionMutations() {
    if (pendingSessionMutations === 0) return Promise.resolve();
    return new Promise((resolve) => sessionMutationWaiters.push(resolve));
  }
  function notifyTool(message) {
    clearTimeout(toolNoticeTimer);
    setToolNotice(message);
    toolNoticeTimer = setTimeout(() => setToolNotice(""), 1800);
  }

  function startCreatingSession() {
    if (sessionMenuRef) sessionMenuRef.open = false;
    setCreatingSession(true);
    setSessionError("");
    queueMicrotask(() => document.getElementById("terminal-create-session-name")?.focus());
  }

  function tabId(name) {
    return `terminal-tab-${encodeURIComponent(props.name + "-" + name).replace(/%/g, "_")}`;
  }

  function panelId() {
    return `terminal-panel-${encodeURIComponent(props.name).replace(/%/g, "_")}`;
  }

  function setSessionsFromResponse(body) {
    const next = (Array.isArray(body?.sessions) ? body.sessions : []).map(normalizeSession).filter(Boolean);
    setSessions(next);
    props.onSessionsChange?.(next);
    if (!next.some((session) => session.name === selectedName())) {
      const replacement = next.find((session) => session.name === "main") || next[0];
      setSelectedName(replacement?.name || "");
    }
    return next;
  }

  async function loadSessions({ announce = false, force = false } = {}) {
    if (force) {
      sessionLoadSequence += 1;
      sessionsLoadPromise = undefined;
    } else if (!announce && sessionsLoadPromise) return sessionsLoadPromise;
    const operation = (async () => {
      await waitForSessionMutations();
      if (disposed) return [];
      const loadSequence = ++sessionLoadSequence;
      if (!sessions().length) setListState("loading");
      try {
        const body = await api(`${projectPath()}/sessions`);
        if (disposed || loadSequence !== sessionLoadSequence) return [];
        const next = setSessionsFromResponse(body);
        setListState("ready");
        setSessionError("");
        if (announce) setStatusText("Session list refreshed.");
        return next;
      } catch (error) {
        if (!disposed && loadSequence === sessionLoadSequence) {
          setListState("error");
          setSessionError(error?.message || "Sessions could not be loaded.");
        }
        return [];
      }
    })();
    if (!announce) sessionsLoadPromise = operation;
    try {
      return await operation;
    } finally {
      if (sessionsLoadPromise === operation) sessionsLoadPromise = undefined;
    }
  }


  function send(type, { flags = 0, streamId = 0, sequence, payload } = {}) {
    if (ws?.readyState !== WebSocket.OPEN) return false;
    ws.send(encodeFrame({
      type,
      flags,
      streamId,
      sequence: sequence ?? ++controlSequence,
      payload
    }));
    return true;
  }

  function clearPendingLiveWrites(stream) {
    if (!stream) return;
    clearTimeout(stream.liveWriteTimer);
    stream.liveWriteTimer = undefined;
    stream.liveWriteQueue.length = 0;
  }

  function closeStream(streamId = activeStreamId) {
    if (!streamId) return;
    const wasActive = activeStreamId === streamId;
    const stream = streams.get(streamId);
    clearPendingLiveWrites(stream);
    if (stream) stream.closed = true;
    send(TYPES.CLOSE_STREAM, { streamId, sequence: 0, payload: new Uint8Array() });
    streams.delete(streamId);
    if (wasActive) {
      activeStreamId = 0;
      inputEnabled = false;
    }
  }

  function closeAllStreams() {
    for (const streamId of [...streams.keys()]) closeStream(streamId);
    activeStreamId = 0;
    inputEnabled = false;
  }

  function desiredDimensions() {
    if (term?.cols > 0 && term?.rows > 0) return { cols: term.cols, rows: term.rows };
    try {
      const proposed = fit?.proposeDimensions?.();
      if (proposed?.cols > 0 && proposed?.rows > 0) return proposed;
    } catch {}
    return { cols: 80, rows: 24 };
  }

  function sendResize() {
    const stream = streams.get(activeStreamId);
    if (!stream || !helloAcknowledged) return;
    const { cols, rows } = desiredDimensions();
    if (cols < 1 || rows < 1 || (cols === stream.cols && rows === stream.rows)) return;
    if (send(TYPES.RESIZE, {
      streamId: activeStreamId,
      sequence: 0,
      payload: encodeResize(cols, rows)
    })) {
      stream.cols = cols;
      stream.rows = rows;
    }
  }
  function beginResizeOutputSettlement() {
    resizeOutputSettling = true;
    clearTimeout(resizeOutputTimer);
    const active = streams.get(activeStreamId);
    if (active?.liveWriteTimer !== undefined) {
      clearTimeout(active.liveWriteTimer);
      active.liveWriteTimer = undefined;
    }
    resizeOutputTimer = setTimeout(() => {
      resizeOutputTimer = undefined;
      resizeOutputSettling = false;
      const current = streams.get(activeStreamId);
      if (current) flushLiveTerminalFrames(current);
    }, terminalResizeSettleDelay(roundTripMs()));
  }
  function applyRemoteViewport(stream, cols, rows) {
    cols = Number(cols);
    rows = Number(rows);
    if (!Number.isInteger(cols) || cols < 1 || !Number.isInteger(rows) || rows < 1) return;
    stream.renderCols = cols;
    stream.renderRows = rows;
    if (
      stream.streamId !== activeStreamId ||
      stream.sessionName !== selectedName() ||
      !term
    ) return;
    if (term.cols !== cols || term.rows !== rows) {
      beginResizeOutputSettlement();
      term.resize(cols, rows);
    }
    setDimensions({ cols, rows });
  }

  function clearFitVerificationTimers() {
    for (const timer of fitVerificationTimers) clearTimeout(timer);
    fitVerificationTimers = [];
  }

  function terminalFitNeedsAnotherPass() {
    let proposed;
    try { proposed = fit?.proposeDimensions?.(); } catch { return false; }
    const screen = terminalCanvasRef?.querySelector?.(".xterm-screen");
    const screenRect = screen?.getBoundingClientRect?.();
    return Boolean(proposed && screenRect && shouldRefitTerminal({
      current: { cols: term.cols, rows: term.rows },
      proposed,
      hostRect: { width: hostRef.clientWidth, height: hostRef.clientHeight },
      screenRect: { width: screenRect.width, height: screenRect.height }
    }));
  }

  function verifyTerminalFit() {
    if (!fit || !term || !hostRef || hostRef.clientWidth < 1 || hostRef.clientHeight < 1) return;
    if (terminalFitNeedsAnotherPass()) fitTerminal({ verify: false });
  }

  function queueFitVerification() {
    clearFitVerificationTimers();
    for (const delay of FIT_VERIFY_DELAYS_MS) {
      const timer = setTimeout(() => {
        fitVerificationTimers = fitVerificationTimers.filter((candidate) => candidate !== timer);
        if (!disposed && props.active !== false) verifyTerminalFit();
      }, delay);
      fitVerificationTimers.push(timer);
    }
  }

  function fitTerminal({ verify = true } = {}) {
    if (!fit || !term || !hostRef || hostRef.clientWidth < 1 || hostRef.clientHeight < 1) return;
    for (let pass = 0; pass < 3; pass += 1) {
      try { fit.fit(); } catch { break; }
      if (!terminalFitNeedsAnotherPass()) break;
    }
    setDimensions({ cols: term.cols, rows: term.rows });
    syncMobileScroll();
    beginResizeOutputSettlement();
    sendResize();
    if (verify) queueFitVerification();
  }

  function scheduleFit() {
    clearFitVerificationTimers();
    if (fitFrame) cancelAnimationFrame(fitFrame);
    fitFrame = requestAnimationFrame(() => {
      fitFrame = 0;
      fitTerminal();
    });
  }

  function resetTerminalForOpen(name) {
    inputEnabled = false;
    setOpening(true);
    setConnectionState("loading");
    setHistoryBytes(0);
    setDegraded(false);
    setStatusText(`Loading ${name} history…`);
    const resetSequence = ++terminalResetSequence;
    term?.write("", () => {
      if (disposed || resetSequence !== terminalResetSequence) return;
      term?.reset();
      term?.clear();
      syncMobileScroll();
    });
  }

  function openSession(name, { focus = true } = {}) {
    if (!name || !helloAcknowledged || ws?.readyState !== WebSocket.OPEN) return;
    if (opening()) return;
    if (activeStreamId) closeStream(activeStreamId);
    resetTerminalForOpen(name);
    const requestId = `${Date.now().toString(36)}-${++requestCounter}`;
    const { cols, rows } = desiredDimensions();
    pendingOpens.set(requestId, { name, focus, cols, rows });
    send(TYPES.OPEN, {
      payload: encodeJson({ requestId, project: props.name, sessionName: name, cols, rows })
    });
  }

  function selectSession(session, options = {}) {
    if (!session?.name || session.name === selectedName()) {
      if (connected()) term?.focus();
      else if (session?.name && helloAcknowledged && !opening()) openSession(session.name, options);
      return;
    }
    selectionSequence += 1;
    const attachedName = streams.get(activeStreamId)?.sessionName;
    setSelectedName(session.name);
    if (session.name === attachedName && connected()) {
      term?.focus();
      return;
    }
    if (opening()) {
      resetTerminalForOpen(session.name);
      if (!activeStreamId) return;
      closeStream(activeStreamId);
      setOpening(false);
    }
    openSession(session.name, options);
  }

  function maybeEnableInput(stream) {
    if (!stream || stream.closed || streams.get(stream.streamId) !== stream || !stream.readyReceived || stream.pendingHistoryWrites !== 0 || stream.streamId !== activeStreamId || stream.sessionName !== selectedName()) return;
    inputEnabled = true;
    setOpening(false);
    setConnectionState("ready");
    setStatusText(`Connected to ${sessionLabel(activeSession())}.`);
    requestAnimationFrame(() => {
      if (props.active === false || stream.closed || streams.get(stream.streamId) !== stream) return;
      scheduleFit();
      if (stream.focusOnReady) term?.focus();
    });
  }

  function acknowledgeConsumedBatch(stream, frames) {
    if (stream.closed || streams.get(stream.streamId) !== stream) return;
    for (const frame of frames) stream.consumed.add(frame.sequence);
    while (stream.consumed.delete(stream.highestConsumed + 1)) stream.highestConsumed += 1;
    send(TYPES.ACK, { streamId: stream.streamId, sequence: stream.highestConsumed, payload: new Uint8Array() });
  }

  function acknowledgeConsumed(stream, sequence) {
    acknowledgeConsumedBatch(stream, [{ sequence }]);
  }

  function writeTerminalFrame(stream, frame, isHistory) {
    if (isHistory) {
      stream.pendingHistoryWrites += 1;
      setHistoryBytes((value) => value + frame.payload.byteLength);
    }
    const complete = () => {
      acknowledgeConsumed(stream, frame.sequence);
      if (isHistory) {
        stream.pendingHistoryWrites -= 1;
        maybeEnableInput(stream);
      }
    };
    if (stream.streamId === activeStreamId && stream.sessionName === selectedName() && term) term.write(frame.payload, complete);
    else complete();
  }

  function flushLiveTerminalFrames(stream) {
    stream.liveWriteTimer = undefined;
    const frames = stream.liveWriteQueue.splice(0);
    if (!frames.length || stream.closed) return;
    const complete = () => acknowledgeConsumedBatch(stream, frames);
    if (stream.streamId === activeStreamId && stream.sessionName === selectedName() && term) {
      term.write(joinTerminalPayloads(frames.map((frame) => frame.payload)), complete);
    } else complete();
  }

  function queueLiveTerminalFrame(stream, frame) {
    if (stream.closed) return;
    stream.liveWriteQueue.push(frame);
    if (stream.liveWriteTimer !== undefined) return;
    if (resizeOutputSettling) return;
    if (document.visibilityState !== "visible") {
      stream.liveWriteTimer = 0;
      queueMicrotask(() => flushLiveTerminalFrames(stream));
      return;
    }
    stream.liveWriteTimer = setTimeout(() => flushLiveTerminalFrames(stream), LIVE_WRITE_BATCH_MS);
  }

  function handleOpened(frame) {
    const message = decodeJson(frame.payload);
    const pending = pendingOpens.get(message.requestId);
    pendingOpens.delete(message.requestId);
    if (!pending) {
      send(TYPES.CLOSE_STREAM, { streamId: frame.streamId, sequence: 0, payload: new Uint8Array() });
      return;
    }
    if (pending.name !== selectedName() || message.sessionName !== pending.name || !sessions().some((session) => session.name === pending.name)) {
      send(TYPES.CLOSE_STREAM, { streamId: frame.streamId, sequence: 0, payload: new Uint8Array() });
      setOpening(false);
      const target = sessions().find((session) => session.name === selectedName());
      if (target) queueMicrotask(() => openSession(target.name, { focus: pending.focus }));
      return;
    }
    const stream = {
      streamId: frame.streamId,
      sessionName: message.sessionName,
      inputSequence: 0,
      cols: pending.cols,
      rows: pending.rows,
      renderCols: Number(message.cols) || pending.cols,
      renderRows: Number(message.rows) || pending.rows,
      pendingHistoryWrites: 0,
      readyReceived: false,
      highestConsumed: frame.sequence,
      consumed: new Set(),
      focusOnReady: pending.focus,
      liveWriteQueue: [],
      liveWriteTimer: undefined,
      closed: false
    };
    streams.set(frame.streamId, stream);
    activeStreamId = frame.streamId;
    applyRemoteViewport(stream, stream.renderCols, stream.renderRows);
    setDegraded(Boolean(message.degraded));
    const next = sessions().map((session) => session.name === message.sessionName
      ? { ...session, state: "running" }
      : session);
    setSessions(next);
    props.onSessionsChange?.(next);
  }

  function applyDeletedSession(session, { recordRevision = true } = {}) {
    const deletedName = session.name;
    if (recordRevision) bumpSessionRevision(deletedName);
    sessionLoadSequence += 1;
    const current = sessions();
    if (!current.some((item) => item.name === deletedName)) return false;
    const deletedTab = tabRefs.get(deletedName);
    const focusedControl = document.activeElement;
    const preserveControlFocus = Boolean(
      focusedControl &&
      workspaceRef?.contains(focusedControl) &&
      !focusedControl.closest(".xterm") &&
      !deletedTab?.contains(focusedControl)
    );
    const restoreTabFocus = userDeletionFocus.has(deletedName) ||
      Boolean(deletedTab?.parentElement?.contains(document.activeElement));
    const wasSelected = selectedName() === deletedName;
    if (wasSelected) {
      pendingOpens.clear();
      closeAllStreams();
      setOpening(false);
    }
    const next = current.filter((item) => item.name !== deletedName);
    setSessions(next);
    setListState("ready");
    props.onSessionsChange?.(next);
    setStatusText(`Session ${sessionLabel(session)} was deleted.`);
    if (!wasSelected) return true;

    const replacement = next.find((item) => item.name === "main") || next[0];
    setSelectedName(replacement?.name || "");
    if (replacement) {
      openSession(replacement.name, { focus: !restoreTabFocus && !preserveControlFocus });
      if (restoreTabFocus) queueMicrotask(() => tabRefs.get(replacement.name)?.focus());
    } else {
      setConnectionState("offline");
      setStatusText("No persistent terminal sessions remain. Create a session to continue.");
      if (!preserveControlFocus) queueMicrotask(() => document.getElementById("terminal-create-session")?.focus());
    }
    return true;
  }

  function handleSessionEvent(frame) {
    const message = decodeJson(frame.payload);
    if (message.project !== props.name) return;
    if (message.event === "activity") {
      const name = String(message.session?.name || "");
      if (!SESSION_NAME.test(name)) return;
      const attachedClients = Number(message.session?.attachedClients);
      const lastInteractionAt = message.session?.lastInteractionAt;
      const next = sessions().map((item) => item.name === name ? {
        ...item,
        ...(Number.isSafeInteger(attachedClients) && attachedClients >= 0 ? { attachedClients } : {}),
        ...(typeof lastInteractionAt === "string" ? { lastInteractionAt } : {}),
        ...(typeof message.session?.state === "string" ? { state: message.session.state } : {})
      } : item);
      setSessions(next);
      props.onSessionsChange?.(next);
      return;
    }
    const session = normalizeSession(message.session);
    if (!session || !SESSION_NAME.test(session.name)) return;
    if (message.event !== "created" && message.event !== "updated" && message.event !== "deleted") return;

    bumpSessionRevision(session.name);
    if (message.event === "created" || message.event === "updated") {
      sessionLoadSequence += 1;
      const current = sessions();
      const index = current.findIndex((item) => item.name === session.name);
      const next = index === -1
        ? [...current, session]
        : current.map((item, itemIndex) => itemIndex === index ? session : item);
      setSessions(next);
      if (!selectedName()) {
        const replacement = next.find((item) => item.name === "main") || next[0];
        setSelectedName(replacement?.name || "");
      }
      setListState("ready");
      props.onSessionsChange?.(next);
      return;
    }

    applyDeletedSession(session, { recordRevision: false });
  }

  function handleStatus(frame, stream = null) {
    const message = decodeJson(frame.payload);
    if (message.code === "INVALID_HELLO" && /authentication/i.test(message.message || "")) {
      refreshCsrfOnReconnect = true;
    }
    if (message.degraded != null) setDegraded(Boolean(message.degraded));
    if (message.state === "viewport" && stream) {
      applyRemoteViewport(stream, message.cols, message.rows);
    }
    if (message.state === "error" || (frame.flags & FLAGS.ERROR)) {
      inputEnabled = false;
      setOpening(false);
      setConnectionState("error");
      setStatusText(message.message || "Terminal stream failed.");
      return;
    }
    if (message.message) setStatusText(message.message);
  }

  function handleFrame(frame) {
    switch (frame.type) {
      case TYPES.HELLO_ACK: {
        const message = decodeJson(frame.payload);
        if (message.protocol !== "RTP/1") throw new Error("Server did not accept RTP/1");
        transportConnectedOnce = true;
        helloAcknowledged = true;
        reconnectAttempt = 0;
        heartbeatMs = Number(message.heartbeatMs) || HEARTBEAT_MS;
        setConnectionState("loading");
        setStatusText("Terminal transport connected.");
        startHeartbeat();
        const availableSessions = loadSessions({ force: true });
        void availableSessions.then((loaded) => {
          if (!helloAcknowledged || ws?.readyState !== WebSocket.OPEN) return;
          const current = loaded.length ? loaded : sessions();
          const target = current.find((session) => session.name === selectedName()) ||
            current.find((session) => session.name === "main") ||
            current[0];
          if (target) {
            setSelectedName(target.name);
            openSession(target.name, { focus: false });
          } else {
            setOpening(false);
            setConnectionState("offline");
            setStatusText("No persistent terminal sessions remain.");
          }
        });
        return;
      }
      case TYPES.OPENED:
        handleOpened(frame);
        return;
      case TYPES.HISTORY: {
        const stream = streams.get(frame.streamId);
        if (!stream) return;
        writeTerminalFrame(stream, frame, true);
        return;
      }
      case TYPES.READY: {
        const stream = streams.get(frame.streamId);
        if (!stream) return;
        const ready = decodeJson(frame.payload);
        applyRemoteViewport(stream, ready.cols, ready.rows);
        stream.readyReceived = true;
        acknowledgeConsumed(stream, frame.sequence);
        maybeEnableInput(stream);
        return;
      }
      case TYPES.OUTPUT: {
        const stream = streams.get(frame.streamId);
        if (stream) queueLiveTerminalFrame(stream, frame);
        return;
      }
      case TYPES.CLOSE_STREAM: {
        const stream = streams.get(frame.streamId);
        if (stream) stream.closed = true;
        streams.delete(frame.streamId);
        if (activeStreamId === frame.streamId) {
          activeStreamId = 0;
          inputEnabled = false;
          setOpening(false);
          setConnectionState(frame.flags & FLAGS.ERROR ? "error" : "offline");
          const detail = frame.payload.byteLength ? decodeJson(frame.payload) : null;
          setStatusText(detail?.message || "Terminal stream detached.");
        }
        return;
      }
      case TYPES.STATUS: {
        const stream = frame.streamId ? streams.get(frame.streamId) : null;
        if (frame.streamId && !stream) return;
        if (!frame.streamId || stream.streamId === activeStreamId) handleStatus(frame, stream);
        if (stream) acknowledgeConsumed(stream, frame.sequence);
        return;
      }
      case TYPES.PROTOCOL_ERROR:
        handleStatus(frame);
        return;
      case TYPES.SESSION_EVENT:
        handleSessionEvent(frame);
        return;
      case TYPES.PING:
        send(TYPES.PONG, { sequence: frame.sequence, payload: frame.payload });
        return;
      case TYPES.PONG: {
        const sentAt = Number(decodePing(frame.payload));
        missedPongs = 0;
        setRoundTripMs(Math.max(0, Date.now() - sentAt));
        return;
      }
      case TYPES.ACK:
        return;
      default:
        throw new Error(`Unsupported RTP frame type ${frame.type}`);
    }
  }

  function startHeartbeat() {
    clearInterval(heartbeatTimer);
    missedPongs = 0;
    heartbeatTimer = setInterval(() => {
      if (ws?.readyState !== WebSocket.OPEN) return;
      if (missedPongs >= 2) {
        ws.close(4000, "Heartbeat timeout");
        return;
      }
      missedPongs += 1;
      send(TYPES.PING, { payload: encodePing(Date.now()) });
    }, heartbeatMs);
  }

  function scheduleReconnect() {
    if (disposed || reconnectTimer) return;
    const base = Math.min(1500, 250 * (2 ** reconnectAttempt++));
    const delay = Math.round(base * (0.8 + Math.random() * 0.4));
    setConnectionState("offline");
    setStatusText(`Terminal offline. Reconnecting in ${delay} ms…`);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;
      void connectWebSocket();
    }, delay);
  }

  async function connectWebSocket() {
    transportAttemptCount += 1;
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
    helloAcknowledged = false;
    inputEnabled = false;
    setConnectionState(reconnectAttempt ? "reconnecting" : "connecting");
    setStatusText(reconnectAttempt ? "Reconnecting to terminal…" : "Connecting to terminal…");
    try {
      const csrfToken = await terminalCsrfToken();
      if (disposed) return;
      const socket = new WebSocket(terminalWebSocketUrl());
      ws = socket;
      socket.binaryType = "arraybuffer";
      socket.onopen = () => {
        if (ws !== socket || disposed) return;
        controlSequence = 0;
        send(TYPES.HELLO, {
          payload: encodeJson({ csrfToken, clientVersion: "1", capabilities: ["binary", "multiplex", "history"] })
        });
      };
      socket.onmessage = async (event) => {
        if (ws !== socket || disposed) return;
        try {
          const bytes = event.data instanceof ArrayBuffer
            ? event.data
            : event.data instanceof Blob
              ? await event.data.arrayBuffer()
              : null;
          if (ws !== socket || disposed) return;
          if (!bytes) throw new Error("Text WebSocket messages are not valid RTP frames");
          handleFrame(decodeFrame(bytes, DIRECTIONS.SERVER_TO_CLIENT));
        } catch (error) {
          if (ws !== socket || disposed) return;
          setConnectionState("error");
          setStatusText(error?.message || "Terminal protocol error.");
          socket.close(1002, "Protocol error");
        }
      };
      socket.onerror = () => {
        if (ws !== socket || disposed) return;
        setConnectionState("error");
        setStatusText("Terminal connection failed.");
      };
      socket.onclose = (event) => {
        if (ws !== socket) return;
        clearInterval(heartbeatTimer);
        helloAcknowledged = false;
        closeAllStreams();
        pendingOpens.clear();
        setOpening(false);
        setRoundTripMs(null);
        if (refreshCsrfOnReconnect) {
          invalidateTerminalCsrfToken();
          refreshCsrfOnReconnect = false;
        }
        if (event.code === 4003) {
          invalidateTerminalCsrfToken();
          const next = window.location.pathname + window.location.search + window.location.hash;
          window.dispatchEvent(new CustomEvent("reaper:unauthorized", { detail: { next } }));
          return;
        }
        if (!disposed) scheduleReconnect();
      };
    } catch (error) {
      setConnectionState("error");
      setStatusText(error?.message || "Terminal connection failed.");
      scheduleReconnect();
    }
  }

  function sendInputBytes(bytes, streamId = activeStreamId) {
    const stream = streams.get(streamId);
    if (!stream || streamId !== activeStreamId || !inputEnabled || ws?.readyState !== WebSocket.OPEN) return;
    for (let offset = 0; offset < bytes.byteLength; offset += INPUT_MAX_PAYLOAD) {
      send(TYPES.INPUT, {
        streamId,
        sequence: ++stream.inputSequence,
        payload: bytes.subarray(offset, offset + INPUT_MAX_PAYLOAD)
      });
    }
  }


  function queueInput(data) {
    if (!inputEnabled || !data) return;
    sendInputBytes(encoder.encode(data));
  }

  function syncMobileScroll(position) {
    const buffer = term?.buffer?.active;
    if (!buffer) {
      setMobileScrollMaximum(0);
      setMobileScrollPosition(0);
      return;
    }
    const maximum = Math.max(0, Math.round(Number(buffer.baseY) || 0));
    const nextPosition = position == null ? Number(buffer.viewportY) : Number(position);
    setMobileScrollMaximum(maximum);
    setMobileScrollPosition(Math.max(0, Math.min(maximum, Math.round(nextPosition) || 0)));
  }

  function inputTerminalKey(sequence) {
    if (!connected() || !sequence || !term) return;
    term.clearSelection();
    term.scrollToBottom();
    syncMobileScroll();
    term.input(sequence, false);
  }

  function inputTerminalArrow(finalByte) {
    const prefix = term?.modes?.applicationCursorKeysMode ? "\x1bO" : "\x1b[";
    inputTerminalKey(`${prefix}${finalByte}`);
  }

  function scrollTerminal(action) {
    if (!term) return;
    if (action === "top") term.scrollToTop();
    else if (action === "page-up") term.scrollPages(-1);
    else if (action === "page-down") term.scrollPages(1);
    else if (action === "bottom") term.scrollToBottom();
    syncMobileScroll();
  }

  function setTerminalScrollPosition(value) {
    if (!term) return;
    term.scrollToLine(Math.max(0, Math.min(mobileScrollMaximum(), Math.round(Number(value)) || 0)));
    syncMobileScroll();
  }

  async function createSession(event) {
    event?.preventDefault();
    const name = newSessionName().trim().toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "");
    if (!SESSION_NAME.test(name)) {
      setSessionError("Session names must be 1–32 lowercase letters, numbers, or hyphens.");
      return;
    }
    if (sessions().some((session) => session.name === name)) {
      setSessionError(`Session “${name}” already exists.`);
      tabRefs.get(name)?.focus();
      return;
    }
    const selectedAtSubmit = selectedName();
    const selectionAtSubmit = selectionSequence;
    const revisionAtSubmit = sessionRevision(name);
    beginSessionMutation();
    setBusyName(name);
    setSessionError("");
    try {
      const response = await api(`${projectPath()}/sessions`, { method: "POST", body: JSON.stringify({ name }) });
      const session = response?.session && typeof response.session === "object" && !Array.isArray(response.session)
        ? normalizeSession(response.session)
        : null;
      if (!session || session.name !== name || !SESSION_NAME.test(session.name)) {
        throw new Error("Created session response was invalid.");
      }
      let next = sessions();
      let appliedResponse = false;
      if (sessionRevision(name) === revisionAtSubmit) {
        sessionLoadSequence += 1;
        const existingIndex = next.findIndex((item) => item.name === session.name);
        next = existingIndex === -1
          ? [...next, session]
          : next.map((item, index) => index === existingIndex ? session : item);
        setSessions(next);
        setListState("ready");
        props.onSessionsChange?.(next);
        appliedResponse = true;
      }
      const available = next.find((item) => item.name === session.name);
      const shouldActivate = Boolean(available) &&
        selectionSequence === selectionAtSubmit &&
        (selectedName() === selectedAtSubmit || (selectedAtSubmit === "" && selectedName() === available.name));
      if (shouldActivate) setSelectedName(available.name);
      setCreatingSession(false);
      setNewSessionName("");
      if (shouldActivate) {
        queueMicrotask(() => {
          tabRefs.get(available.name)?.focus();
          openSession(available.name);
        });
      } else if (!appliedResponse && !available) {
        setStatusText(`Session ${session.name} was removed before it could be opened.`);
      }
    } catch (error) {
      setSessionError(error?.message || "Session could not be created.");
    } finally {
      endSessionMutation();
      setBusyName("");
    }
  }

  function beginRename(session) {
    setEditingName(session.name);
    setEditingTitle(sessionLabel(session));
    setSessionError("");
    if (sessionMenuRef) sessionMenuRef.open = false;
    queueMicrotask(() => document.getElementById(`terminal-rename-${tabId(session.name)}`)?.select());
  }
  function cancelRename(session) {
    setEditingName("");
    setEditingTitle("");
    queueMicrotask(() => tabRefs.get(session.name)?.focus());
  }


  async function saveRename(event, session) {
    event.preventDefault();
    const title = editingTitle().trim();
    if (!title || title.length > 48) {
      setSessionError("Session titles must be 1–48 characters.");
      return;
    }
    const revisionAtSubmit = sessionRevision(session.name);
    beginSessionMutation();
    setBusyName(session.name);
    try {
      const response = await api(`${projectPath()}/sessions/${encodeURIComponent(session.name)}`, {
        method: "PATCH",
        body: JSON.stringify({ title })
      });
      const updated = response?.session && typeof response.session === "object" && !Array.isArray(response.session)
        ? normalizeSession(response.session)
        : null;
      if (!updated || updated.name !== session.name) throw new Error("Updated session response was invalid.");
      if (sessionRevision(session.name) === revisionAtSubmit) {
        sessionLoadSequence += 1;
        const next = sessions().map((item) => item.name === updated.name ? updated : item);
        setSessions(next);
        props.onSessionsChange?.(next);
      }
      setEditingName("");
      setSessionError("");
      queueMicrotask(() => tabRefs.get(session.name)?.focus());
    } catch (error) {
      setSessionError(error?.message || "Session title could not be saved.");
    } finally {
      endSessionMutation();
      setBusyName("");
    }
  }

  async function deleteSession(session) {
    if (!window.confirm(`Delete persistent session “${sessionLabel(session)}” (${session.name})? This stops every process running in it.`)) return;
    const revisionAtSubmit = sessionRevision(session.name);
    beginSessionMutation();
    setBusyName(session.name);
    setSessionError("");
    userDeletionFocus.add(session.name);
    try {
      await api(`${projectPath()}/sessions/${encodeURIComponent(session.name)}`, { method: "DELETE" });
      if (sessionMenuRef) sessionMenuRef.open = false;
      if (sessionRevision(session.name) === revisionAtSubmit) applyDeletedSession(session);
    } catch (error) {
      setSessionError(error?.message || "Session could not be deleted.");
      queueMicrotask(() => tabRefs.get(session.name)?.focus());
    } finally {
      endSessionMutation();
      userDeletionFocus.delete(session.name);
      setBusyName("");
    }
  }

  function handleTabKeyDown(event, name) {
    const items = sessions();
    const index = items.findIndex((session) => session.name === name);
    let nextIndex;
    if (event.key === "ArrowRight") nextIndex = (index + 1) % items.length;
    else if (event.key === "ArrowLeft") nextIndex = (index - 1 + items.length) % items.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = items.length - 1;
    else return;
    event.preventDefault();
    const next = items[nextIndex];
    tabRefs.get(next.name)?.focus();
    selectSession(next, { focus: false });
  }

  function hideOutsideWorkspace() {
    outsideState = [];
    let branch = workspaceRef;
    while (branch && branch !== document.body) {
      const parent = branch.parentElement;
      if (!parent) break;
      for (const sibling of parent.children) {
        if (sibling === branch) continue;
        outsideState.push({
          element: sibling,
          inert: sibling.hasAttribute("inert"),
          ariaHidden: sibling.getAttribute("aria-hidden")
        });
        sibling.setAttribute("inert", "");
        sibling.setAttribute("aria-hidden", "true");
      }
      branch = parent;
    }
  }

  function restoreOutsideWorkspace() {
    for (const state of outsideState.reverse()) {
      if (!state.inert) state.element.removeAttribute("inert");
      if (state.ariaHidden === null) state.element.removeAttribute("aria-hidden");
      else state.element.setAttribute("aria-hidden", state.ariaHidden);
    }
    outsideState = [];
  }

  function focusableWorkspaceElements() {
    return Array.from(workspaceRef?.querySelectorAll(FOCUSABLE_SELECTOR) || [])
      .filter((element) =>
        element.tabIndex >= 0 &&
        element.getClientRects().length > 0 &&
        !element.closest("[hidden], [inert], [aria-hidden='true']")
      );
  }

  function trapFullscreenTab(event) {
    const focusable = focusableWorkspaceElements();
    if (!focusable.length) {
      event.preventDefault();
      workspaceRef?.focus();
      return;
    }
    const active = document.activeElement;
    if (!workspaceRef?.contains(active)) {
      event.preventDefault();
      (event.shiftKey ? focusable[focusable.length - 1] : focusable[0]).focus();
    } else if (event.shiftKey && active === focusable[0]) {
      event.preventDefault();
      focusable[focusable.length - 1].focus();
    } else if (!event.shiftKey && active === focusable[focusable.length - 1]) {
      event.preventDefault();
      focusable[0].focus();
    }
  }

  function toggleFullscreen(force) {
    const next = typeof force === "boolean" ? force : !expanded();
    if (next === expanded()) return;
    if (next) {
      restoreFocus = document.activeElement;
      hideOutsideWorkspace();
    } else {
      restoreOutsideWorkspace();
    }
    setExpanded(next);
    requestAnimationFrame(() => {
      if (props.active === false) return;
      scheduleFit();
      if (!next) (fullscreenButtonRef || restoreFocus)?.focus?.();
      else term?.focus();
    });
  }

  async function copy() {
    const selection = term?.getSelection();
    if (!selection) {
      notifyTool("Select terminal text to copy.");
      term?.focus();
      return;
    }
    try {
      if (!navigator.clipboard?.writeText) throw new Error("Clipboard access is unavailable");
      await navigator.clipboard.writeText(selection);
      notifyTool("Copied selection.");
    } catch {
      notifyTool("Clipboard access was blocked. Use your browser copy shortcut.");
    }
    term?.focus();
  }

  async function paste() {
    if (!inputEnabled || !activeStreamId) return;
    const streamId = activeStreamId;
    const stream = streams.get(streamId);
    const interactionGeneration = activeInteractionGeneration;
    try {
      if (!navigator.clipboard?.readText) throw new Error("Clipboard access is unavailable");
      const text = await navigator.clipboard.readText();
      if (
        streams.get(streamId) !== stream ||
        activeStreamId !== streamId ||
        !inputEnabled ||
        props.active === false ||
        activeInteractionGeneration !== interactionGeneration
      ) {
        notifyTool("Terminal changed before paste completed; paste canceled.");
        return;
      }
      if (text) term?.paste(text);
      notifyTool(text ? "Pasted from clipboard." : "Clipboard is empty.");
    } catch {
      notifyTool("Clipboard access was blocked. Use your browser paste shortcut.");
    }
    if (props.active !== false && activeInteractionGeneration === interactionGeneration) term?.focus();
  }

  function searchOptions(incremental = false) {
    return { incremental, decorations: SEARCH_DECORATIONS };
  }

  function runFind(direction = "next", incremental = false) {
    const query = findQuery();
    if (!search || !query) {
      search?.clearDecorations();
      setFindResults({ resultIndex: 0, resultCount: 0 });
      return false;
    }
    return direction === "previous"
      ? search.findPrevious(query, searchOptions(false))
      : search.findNext(query, searchOptions(incremental));
  }

  function openFind() {
    if (!search) {
      notifyTool("Search is unavailable in this browser.");
      return;
    }
    setFindOpen(true);
    queueMicrotask(() => {
      findInputRef?.focus();
      findInputRef?.select();
      if (findQuery()) runFind("next", true);
    });
  }

  function closeFind() {
    search?.clearDecorations();
    setFindOpen(false);
    setFindResults({ resultIndex: 0, resultCount: 0 });
    term?.focus();
  }

  function setTerminalFontSize(value) {
    const next = Math.max(MIN_FONT_SIZE, Math.min(MAX_FONT_SIZE, Math.round(value)));
    fontSizeCustomized = true;
    setFontSize(next);
    if (term) term.options.fontSize = next;
    try { window.localStorage.setItem(FONT_SIZE_STORAGE_KEY, String(next)); } catch {}
    scheduleFit();
    notifyTool(`Terminal text set to ${next} px.`);
  }

  function clearView() {
    term?.clear();
    syncMobileScroll();
    notifyTool("Terminal view cleared. Session history is still persistent.");
    term?.focus();
  }

  function retryConnection() {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
    reconnectAttempt = 0;
    clearInterval(heartbeatTimer);
    const previous = ws;
    ws = undefined;
    closeAllStreams();
    pendingOpens.clear();
    try { previous?.close(1000, "Manual reconnect"); } catch {}
    void connectWebSocket();
  }

  createEffect(() => {
    const name = selectedName();
    if (!name) return;
    try { window.localStorage.setItem(selectedSessionStorageKey(), name); } catch {}
  });

  createEffect(() => {
    activeInteractionGeneration += 1;
    if (props.active === false) {
      if (expanded()) {
        restoreOutsideWorkspace();
        setExpanded(false);
        restoreFocus = undefined;
      }
      return;
    }
    scheduleFit();
  });

  onMount(() => {
    try {
      const storedSession = window.localStorage.getItem(selectedSessionStorageKey());
      if (storedSession && SESSION_NAME.test(storedSession)) setSelectedName(storedSession);
    } catch {}
    const mobile = window.matchMedia("(max-width: 720px)");
    let initialFontSize = mobile.matches ? MOBILE_FONT_SIZE : DEFAULT_FONT_SIZE;
    try {
      const storedFontSize = window.localStorage.getItem(FONT_SIZE_STORAGE_KEY);
      if (storedFontSize !== null) {
        const savedFontSize = Number(storedFontSize);
        if (Number.isFinite(savedFontSize)) {
          initialFontSize = Math.max(MIN_FONT_SIZE, Math.min(MAX_FONT_SIZE, Math.round(savedFontSize)));
          fontSizeCustomized = true;
        }
      }
    } catch {}
    setFontSize(initialFontSize);
    term = new Terminal({
      fontSize: initialFontSize,
      fontFamily: '"JetBrains Mono Variable", "JetBrains Mono", "Cascadia Code", "SFMono-Regular", Consolas, monospace',
      fontWeight: "400",
      fontWeightBold: "700",
      lineHeight: 1.08,
      minimumContrastRatio: 4.5,
      screenReaderMode: true,
      theme: {
        background: "#070709", foreground: "#e8e8ec", cursor: "#f1f1f3", cursorAccent: "#070709",
        selectionBackground: "#4b4b5573", selectionForeground: "#ffffff", selectionInactiveBackground: "#30303966",
        black: "#151519", red: "#d4777e", green: "#83ad8f", yellow: "#c3a66f", blue: "#7f9ec4",
        magenta: "#a58ab8", cyan: "#78a8ad", white: "#d7d7dc", brightBlack: "#70707b", brightRed: "#ec9298",
        brightGreen: "#9bc5a6", brightYellow: "#d9bd85", brightBlue: "#9ab8dc", brightMagenta: "#bda2ce",
        brightCyan: "#91c1c5", brightWhite: "#f4f4f6"
      },
      scrollback: 50000,
      scrollSensitivity: 1,
      fastScrollSensitivity: 5,
      fastScrollModifier: "alt",
      scrollOnUserInput: true,
      cursorBlink: true,
      cursorStyle: "block",
      cursorInactiveStyle: "outline",
      customGlyphs: true,
      rescaleOverlappingGlyphs: true,
      drawBoldTextInBrightColors: true,
      macOptionIsMeta: true,
      altClickMovesCursor: true,
      rightClickSelectsWord: true,
      allowProposedApi: true
    });
    fit = new FitAddon();
    term.loadAddon(fit);
    const unicode = new Unicode11Addon();
    term.loadAddon(unicode);
    term.unicode.activeVersion = "11";
    try {
      search = new SearchAddon();
      term.loadAddon(search);
      searchResultDisposable = search.onDidChangeResults((result) => setFindResults(result));
    } catch { search = undefined; }
    term.open(terminalCanvasRef);
    if (document.fonts) {
      fontLoadingHandler = () => {
        if (disposed || !term) return;
        try { term.options.fontFamily = term.options.fontFamily; } catch {}
        scheduleFit();
      };
      document.fonts.addEventListener?.("loadingdone", fontLoadingHandler);
      void document.fonts.ready.then(fontLoadingHandler).catch(() => {});
    }
    term.attachCustomKeyEventHandler((event) => {
      if (event.type !== "keydown") return true;
      const modifier = event.ctrlKey || event.metaKey;
      if (!modifier || event.altKey) return true;
      const key = event.key.toLowerCase();
      if (event.shiftKey && key === "f6") {
        event.preventDefault();
        (tabRefs.get(selectedName()) || fullscreenButtonRef)?.focus();
        notifyTool("Focus moved to terminal tools.");
        return false;
      }
      if (event.shiftKey && key === "f") {
        event.preventDefault();
        openFind();
        return false;
      }
      if (event.shiftKey && key === "c") {
        event.preventDefault();
        void copy();
        return false;
      }
      if (event.shiftKey && key === "v") {
        event.preventDefault();
        void paste();
        return false;
      }
      if (key === "+" || key === "=") {
        event.preventDefault();
        setTerminalFontSize(fontSize() + 1);
        return false;
      }
      if (key === "-") {
        event.preventDefault();
        setTerminalFontSize(fontSize() - 1);
        return false;
      }
      if (key === "0") {
        event.preventDefault();
        setTerminalFontSize(mobile.matches ? MOBILE_FONT_SIZE : DEFAULT_FONT_SIZE);
        return false;
      }
      return true;
    });

    const resizeObserver = new ResizeObserver(scheduleFit);
    resizeObserver.observe(workspaceRef);
    resizeObserver.observe(hostRef);
    const onResize = () => scheduleFit();
    window.addEventListener("resize", onResize);
    window.visualViewport?.addEventListener("resize", onResize);
    terminalEventDisposables = [
      term.onData(queueInput),
      term.onSelectionChange(() => setHasSelection(term.hasSelection())),
      term.onScroll((position) => syncMobileScroll(position)),
      term.onWriteParsed(() => syncMobileScroll())
    ];
    syncMobileScroll();
    const onMobileChange = (event) => {
      if (fontSizeCustomized) return;
      const next = event.matches ? MOBILE_FONT_SIZE : DEFAULT_FONT_SIZE;
      setFontSize(next);
      term.options.fontSize = next;
      scheduleFit();
    };
    mobile.addEventListener?.("change", onMobileChange);
    documentKeyHandler = (event) => {
      if (props.active === false) return;
      const target = event.target instanceof Element ? event.target : null;
      if (event.key === "Escape" && !target?.closest(".xterm")) {
        const details = target?.closest("details[open]") || workspaceRef?.querySelector("details[open]");
        if (details) {
          event.preventDefault();
          details.open = false;
          details.querySelector("summary")?.focus();
          return;
        }
      }
      if (!expanded() || event.key !== "Tab" || target?.closest(".xterm")) return;
      trapFullscreenTab(event);
    };
    documentFocusHandler = (event) => {
      if (props.active === false) return;
      if (expanded() && workspaceRef && !workspaceRef.contains(event.target)) {
        const focusable = focusableWorkspaceElements();
        (focusable[0] || workspaceRef)?.focus();
      }
    };
    documentPointerHandler = (event) => {
      if (props.active === false) return;
      for (const details of workspaceRef?.querySelectorAll("details[open]") || []) {
        if (!details.contains(event.target)) details.open = false;
      }
    };
    document.addEventListener("keydown", documentKeyHandler);
    document.addEventListener("focusin", documentFocusHandler);
    document.addEventListener("pointerdown", documentPointerHandler, true);
    void loadSessions().then((loaded) => {
      if (helloAcknowledged && !activeStreamId && loaded[0]) openSession(selectedName() || loaded[0].name, { focus: false });
    });
    void connectWebSocket();
    requestAnimationFrame(scheduleFit);

    onCleanup(() => {
      disposed = true;
      clearTimeout(reconnectTimer);
      clearTimeout(toolNoticeTimer);
      clearTimeout(resizeOutputTimer);
      clearFitVerificationTimers();
      document.fonts?.removeEventListener?.("loadingdone", fontLoadingHandler);
      clearInterval(heartbeatTimer);
      if (fitFrame) cancelAnimationFrame(fitFrame);
      resizeObserver.disconnect();
      searchResultDisposable?.dispose();
      for (const disposable of terminalEventDisposables.splice(0)) disposable.dispose();
      mobile.removeEventListener?.("change", onMobileChange);
      document.removeEventListener("keydown", documentKeyHandler);
      document.removeEventListener("focusin", documentFocusHandler);
      document.removeEventListener("pointerdown", documentPointerHandler, true);
      restoreOutsideWorkspace();
      window.removeEventListener("resize", onResize);
      window.visualViewport?.removeEventListener("resize", onResize);
      try { closeAllStreams(); } catch {}
      try { ws?.close(1000, "Terminal unmounted"); } catch {}
      try { term?.dispose(); } catch {}
    });
  });

  return (
    <div class="terminal-surface">
      <div
        ref={workspaceRef}
        class="terminal-workspace"
        classList={{ "terminal-workspace--fullscreen": expanded() }}
        tabIndex="-1"
      >
        <Show when={degraded()}>
          <div class="terminal-degraded" role="status">Temporary fallback shell — processes and output will not persist.</div>
        </Show>
        <div class="terminal-shell">
          <div class="terminal-tabs">
            <div class="terminal-tabs__list" role="tablist" aria-label="Persistent terminal sessions">
              <For each={sessions()}>{(session) => {
                const selected = () => selectedName() === session.name;
                const exceptionalState = () => !["ready", "running"].includes(session.state);
                return (
                  <button
                    ref={(element) => element ? tabRefs.set(session.name, element) : tabRefs.delete(session.name)}
                    id={tabId(session.name)}
                    class="terminal-tab"
                    classList={{ "terminal-tab--active": selected() }}
                    type="button"
                    role="tab"
                    tabIndex={selected() ? 0 : -1}
                    aria-controls={panelId()}
                    aria-selected={selected()}
                    aria-label={`${sessionLabel(session)}, ${session.state}`}
                    disabled={busyName() === session.name}
                    onClick={() => selectSession(session)}
                    onDblClick={() => beginRename(session)}
                    onKeyDown={(event) => handleTabKeyDown(event, session.name)}
                  >
                    <span class="terminal-tab__label">{sessionLabel(session)}</span>
                    <span class={`terminal-tab__state terminal-tab__state--${session.state}`} aria-hidden="true"></span>
                    <Show when={exceptionalState()}>
                      <span class="terminal-tab__state-label">{session.state}</span>
                    </Show>
                  </button>
                );
              }}</For>
            </div>
            <div class="terminal-tabs__actions" role="group" aria-label="Terminal session actions">
              <span
                class="terminal-connection"
                classList={{
                  "terminal-connection--ready": connected(),
                  "terminal-connection--error": connectionState() === "error"
                }}
                title={statusText()}
              >
                <span class="terminal-connection__dot" aria-hidden="true"></span>
                <span>{connectionLabel()}</span>
              </span>
              <button
                id="terminal-create-session"
                class="terminal-rail-action terminal-rail-action--primary"
                type="button"
                disabled={Boolean(busyName())}
                onClick={startCreatingSession}
              >{creatingSession() && busyName() ? "Creating…" : "New session"}</button>
              <Show when={activeSession()}>
                <details ref={sessionMenuRef} class="terminal-session-menu">
                  <summary class="terminal-rail-action" aria-label="Session actions">Session</summary>
                  <div class="terminal-session-menu__popover" role="group" aria-label={`Actions for ${sessionLabel(activeSession())}`}>
                    <span class="terminal-session-menu__eyebrow">Active session</span>
                    <strong class="terminal-session-menu__title">{sessionLabel(activeSession())}</strong>
                    <span class="terminal-session-menu__name">{activeSession().name}</span>
                    <button class="terminal-session-menu__action" type="button" disabled={Boolean(busyName())} onClick={() => beginRename(activeSession())}>Rename session</button>
                    <button class="terminal-session-menu__action terminal-session-menu__action--danger" type="button" disabled={Boolean(busyName())} onClick={() => void deleteSession(activeSession())}>
                      {busyName() === activeSession().name ? "Deleting…" : "Delete session…"}
                    </button>
                    <A class="terminal-session-menu__link" href={`/projects/${encodeURIComponent(props.name)}/sessions`}>Manage all sessions</A>
                  </div>
                </details>
              </Show>
              <button
                ref={fullscreenButtonRef}
                class="terminal-rail-action"
                type="button"
                onClick={() => toggleFullscreen()}
                aria-pressed={expanded()}
                aria-label={expanded() ? "Collapse terminal workspace" : "Expand terminal workspace"}
              >{expanded() ? "Collapse" : "Expand"}</button>
            </div>
          </div>

          <Show when={creatingSession()}>
            <form class="terminal-session-form" aria-busy={Boolean(busyName())} onSubmit={(event) => void createSession(event)}>
              <label class="terminal-session-form__label" for="terminal-create-session-name">New persistent session</label>
              <input
                id="terminal-create-session-name"
                class="terminal-session-form__input"
                value={newSessionName()}
                maxlength="32"
                pattern="[a-z0-9-]{1,32}"
                placeholder="session-name"
                autocomplete="off"
                required
                aria-invalid={Boolean(sessionError())}
                aria-describedby={sessionError() ? "terminal-session-error" : undefined}
                onInput={(event) => setNewSessionName(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    event.preventDefault();
                    setCreatingSession(false);
                    setNewSessionName("");
                    queueMicrotask(() => document.getElementById("terminal-create-session")?.focus());
                  }
                }}
              />
              <span class="terminal-session-form__help">Lowercase letters, numbers, and hyphens.</span>
              <button class="terminal-session-form__button terminal-session-form__button--primary" type="submit" disabled={Boolean(busyName())}>{busyName() ? "Creating…" : "Create session"}</button>
              <button
                class="terminal-session-form__button"
                type="button"
                disabled={Boolean(busyName())}
                onClick={() => {
                  setCreatingSession(false);
                  setNewSessionName("");
                  queueMicrotask(() => document.getElementById("terminal-create-session")?.focus());
                }}
              >Cancel</button>
            </form>
          </Show>

          <Show when={editingSession()}>
            <form class="terminal-session-form" aria-busy={busyName() === editingSession().name} onSubmit={(event) => void saveRename(event, editingSession())}>
              <label class="terminal-session-form__label" for={`terminal-rename-${tabId(editingSession().name)}`}>Rename {editingSession().name}</label>
              <input
                id={`terminal-rename-${tabId(editingSession().name)}`}
                class="terminal-session-form__input"
                value={editingTitle()}
                maxlength="48"
                required
                aria-invalid={Boolean(sessionError())}
                aria-describedby={sessionError() ? "terminal-session-error" : undefined}
                onInput={(event) => setEditingTitle(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    event.preventDefault();
                    cancelRename(editingSession());
                  }
                }}
              />
              <span class="terminal-session-form__help">The shell and running processes are unchanged.</span>
              <button class="terminal-session-form__button terminal-session-form__button--primary" type="submit" disabled={Boolean(busyName())}>{busyName() ? "Saving…" : "Save title"}</button>
              <button class="terminal-session-form__button" type="button" disabled={Boolean(busyName())} onClick={() => cancelRename(editingSession())}>Cancel</button>
            </form>
          </Show>

          <Show when={findOpen()}>
            <form
              class="terminal-findbar"
              role="search"
              aria-label="Find in terminal scrollback"
              onSubmit={(event) => event.preventDefault()}
            >
              <label class="terminal-findbar__label" for="terminal-find-input">Find in scrollback</label>
              <input
                ref={findInputRef}
                id="terminal-find-input"
                class="terminal-findbar__input"
                type="search"
                value={findQuery()}
                autocomplete="off"
                spellcheck="false"
                onInput={(event) => {
                  setFindQuery(event.currentTarget.value);
                  runFind("next", true);
                }}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    event.preventDefault();
                    closeFind();
                  } else if (event.key === "Enter") {
                    event.preventDefault();
                    runFind(event.shiftKey ? "previous" : "next");
                  }
                }}
              />
              <span class="terminal-findbar__results" role="status" aria-live="polite">{findResultLabel()}</span>
              <button class="terminal-findbar__button" type="button" disabled={!findQuery()} onClick={() => runFind("previous")} aria-label="Previous search result">Previous</button>
              <button class="terminal-findbar__button" type="button" disabled={!findQuery()} onClick={() => runFind("next")} aria-label="Next search result">Next</button>
              <button class="terminal-findbar__button terminal-findbar__button--close" type="button" onClick={closeFind}>Done</button>
            </form>
          </Show>

          <Show when={listState() === "error"}>
            <div id="terminal-session-error" class="terminal-session-error" role="alert">
              Sessions could not be loaded. <button class="btn btn--ghost btn--sm" type="button" onClick={() => void loadSessions()}>Retry</button>
            </div>
          </Show>
          <Show when={listState() !== "error" && sessionError()}>
            <div id="terminal-session-error" class="terminal-session-error" role="alert">{sessionError()}</div>
          </Show>

          <div
            ref={hostRef}
            id={panelId()}
            class="terminal-host"
            role="tabpanel"
            tabIndex="-1"
            aria-labelledby={selectedName() ? tabId(selectedName()) : undefined}
            aria-label={selectedName() ? undefined : `Interactive project terminal for ${props.name}`}
            aria-busy={opening()}
            onClick={(event) => {
              if (event.target instanceof Element && event.target.closest(".terminal-mobile-overlay")) return;
              term?.focus();
            }}
          >
            <div ref={terminalCanvasRef} class="terminal-host__canvas"></div>
            <div
              class="terminal-scroll-control terminal-mobile-overlay"
              onClick={(event) => event.stopPropagation()}
            >
              <label class="sr-only" for={`${panelId()}-mobile-scroll`}>Terminal scroll position</label>
              <output class="terminal-scroll-control__value" for={`${panelId()}-mobile-scroll`}>
                {mobileScrollPosition()} / {mobileScrollMaximum()}
              </output>
              <input
                id={`${panelId()}-mobile-scroll`}
                class="terminal-scroll-control__range"
                type="range"
                min="0"
                max={mobileScrollMaximum()}
                value={mobileScrollPosition()}
                disabled={mobileScrollMaximum() === 0}
                aria-valuetext={`Line ${mobileScrollPosition()} of ${mobileScrollMaximum()}`}
                onInput={(event) => setTerminalScrollPosition(event.currentTarget.value)}
              />
            </div>
            <details
              class="terminal-mobile-controls terminal-mobile-overlay"
              onClick={(event) => event.stopPropagation()}
              onToggle={(event) => setMobileControlsOpen(event.currentTarget.open)}
            >
              <summary
                class="terminal-mobile-controls__trigger"
                aria-label={mobileControlsOpen() ? "Close mobile terminal controls" : "Open mobile terminal controls"}
              >
                <svg aria-hidden="true" focusable="false" viewBox="0 0 24 24">
                  <rect x="3" y="5" width="18" height="14" rx="2"></rect>
                  <path d="M7 9h2M11 9h2M15 9h2M7 13h2M11 13h6"></path>
                </svg>
                <span>Keys</span>
              </summary>
              <div class="terminal-mobile-controls__popover">
                <div class="terminal-mobile-controls__section terminal-mobile-controls__section--arrows">
                  <span class="terminal-mobile-controls__title">Cursor keys</span>
                  <div class="terminal-mobile-controls__dpad" role="group" aria-label="Terminal cursor keys">
                    <button class="terminal-mobile-controls__key terminal-mobile-controls__key--up" type="button" disabled={!connected()} onClick={() => inputTerminalArrow("A")} aria-label="Send Up Arrow to terminal">↑</button>
                    <button class="terminal-mobile-controls__key terminal-mobile-controls__key--left" type="button" disabled={!connected()} onClick={() => inputTerminalArrow("D")} aria-label="Send Left Arrow to terminal">←</button>
                    <button class="terminal-mobile-controls__key terminal-mobile-controls__key--down" type="button" disabled={!connected()} onClick={() => inputTerminalArrow("B")} aria-label="Send Down Arrow to terminal">↓</button>
                    <button class="terminal-mobile-controls__key terminal-mobile-controls__key--right" type="button" disabled={!connected()} onClick={() => inputTerminalArrow("C")} aria-label="Send Right Arrow to terminal">→</button>
                  </div>
                </div>
                <div class="terminal-mobile-controls__section">
                  <span class="terminal-mobile-controls__title">Terminal input</span>
                  <div class="terminal-mobile-controls__key-grid" role="group" aria-label="Terminal special keys">
                    <button class="terminal-mobile-controls__key" type="button" disabled={!connected()} onClick={() => inputTerminalKey("\x1b")} aria-label={`Send Escape to ${sessionLabel(activeSession())}`}>Esc</button>
                    <button class="terminal-mobile-controls__key" type="button" disabled={!connected()} onClick={() => inputTerminalKey("\x03")} aria-label={`Send Control+C to ${sessionLabel(activeSession())}`}>Ctrl+C</button>
                    <button class="terminal-mobile-controls__key" type="button" disabled={!connected()} onClick={() => inputTerminalKey("\x04")} aria-label={`Send Control+D to ${sessionLabel(activeSession())}`}>Ctrl+D</button>
                    <button class="terminal-mobile-controls__key" type="button" disabled={!connected()} onClick={() => inputTerminalKey("\r")} aria-label={`Send Enter to ${sessionLabel(activeSession())}`}>Enter</button>
                  </div>
                </div>
                <div class="terminal-mobile-controls__section">
                  <span class="terminal-mobile-controls__title">Scrollback</span>
                  <div class="terminal-mobile-controls__scroll-grid" role="group" aria-label="Terminal scrollback controls">
                    <button class="terminal-mobile-controls__key" type="button" disabled={mobileScrollPosition() === 0} onClick={() => scrollTerminal("top")} aria-label="Scroll terminal to top">Top</button>
                    <button class="terminal-mobile-controls__key" type="button" disabled={mobileScrollPosition() === 0} onClick={() => scrollTerminal("page-up")} aria-label="Scroll terminal up one page">Page ↑</button>
                    <button class="terminal-mobile-controls__key" type="button" disabled={mobileScrollPosition() >= mobileScrollMaximum()} onClick={() => scrollTerminal("page-down")} aria-label="Scroll terminal down one page">Page ↓</button>
                    <button class="terminal-mobile-controls__key" type="button" disabled={mobileScrollPosition() >= mobileScrollMaximum()} onClick={() => scrollTerminal("bottom")} aria-label="Scroll terminal to bottom">Bottom</button>
                  </div>
                </div>
              </div>
            </details>
            <Show when={!connected()}>
              <div class="terminal-state" classList={{ "terminal-state--empty": listState() === "ready" && sessions().length === 0 }}>
                <div
                  class="terminal-state__card"
                  classList={{ "terminal-state__card--error": connectionState() === "error" }}
                  role="status"
                  aria-live="polite"
                >
                  <span class="terminal-state__indicator" aria-hidden="true"></span>
                  <div class="terminal-state__copy">
                    <strong>{connectionHeading()}</strong>
                    <span>{statusText()}</span>
                    <Show when={opening() && historyBytes() > 0}>
                      <span>{Math.round(historyBytes() / 1024)} KiB of scrollback restored</span>
                    </Show>
                    <Show when={(connectionState() === "offline" || connectionState() === "reconnecting" || connectionState() === "error") && sessions().length > 0}>
                      <span class="terminal-state__note">Input is paused. Your process keeps running while this view reconnects.</span>
                    </Show>
                  </div>
                  <Show when={(connectionState() === "offline" || connectionState() === "error") && listState() === "ready"}>
                    <Show when={sessions().length === 0} fallback={
                      <button class="btn btn--outline btn--sm" type="button" onClick={retryConnection}>Retry now</button>
                    }>
                      <button class="btn btn--outline btn--sm" type="button" onClick={startCreatingSession}>New session</button>
                    </Show>
                  </Show>
                </div>
              </div>
            </Show>
          </div>

          <div class="terminal-controls" role="group" aria-label="Terminal tools and connection details">
            <div class="terminal-controls__group" role="group" aria-label="Terminal tools">
              <button class="terminal-control" type="button" disabled={!hasSelection()} onClick={() => void copy()} title="Copy selected text (Ctrl+Shift+C)">Copy</button>
              <button class="terminal-control" type="button" disabled={!connected()} onClick={() => void paste()} title="Paste from clipboard (Ctrl+Shift+V)">Paste</button>
              <button class="terminal-control" type="button" onClick={openFind} title="Find in scrollback (Ctrl+Shift+F)">Find</button>
              <button class="terminal-control" type="button" onClick={clearView} title="Clear this view without stopping the session">Clear</button>
              <div class="terminal-zoom" role="group" aria-label="Terminal text size">
                <button class="terminal-control terminal-control--compact" type="button" disabled={fontSize() <= MIN_FONT_SIZE} onClick={() => setTerminalFontSize(fontSize() - 1)} aria-label="Decrease terminal text size">A−</button>
                <button class="terminal-control terminal-control--value" type="button" onClick={() => setTerminalFontSize(DEFAULT_FONT_SIZE)} aria-label={`Reset terminal text size, currently ${fontSize()} pixels`}>{fontSize()} px</button>
                <button class="terminal-control terminal-control--compact" type="button" disabled={fontSize() >= MAX_FONT_SIZE} onClick={() => setTerminalFontSize(fontSize() + 1)} aria-label="Increase terminal text size">A+</button>
              </div>
              <details class="terminal-key-menu">
                <summary class="terminal-control" aria-label="Send a key to the terminal">Send key</summary>
                <div class="terminal-key-menu__popover" role="group" aria-label="Send a key to the terminal">
                  <button class="terminal-control terminal-control--key" type="button" disabled={!connected()} onClick={() => queueInput("\x03")} aria-label={`Send Control+C to ${sessionLabel(activeSession())}`}>Ctrl+C</button>
                  <button class="terminal-control terminal-control--key" type="button" disabled={!connected()} onClick={() => queueInput("\x04")} aria-label={`Send Control+D to ${sessionLabel(activeSession())}`}>Ctrl+D</button>
                  <button class="terminal-control terminal-control--key" type="button" disabled={!connected()} onClick={() => queueInput("\x1b")} aria-label={`Send Escape to ${sessionLabel(activeSession())}`}>Esc</button>
                  <button class="terminal-control terminal-control--key" type="button" disabled={!connected()} onClick={() => queueInput("\r")} aria-label={`Send Enter to ${sessionLabel(activeSession())}`}>Enter</button>
                </div>
              </details>
            </div>
            <span class="terminal-controls__hint">
              <Show when={toolNotice()}>
                <span class="terminal-controls__notice" role="status">{toolNotice()}</span>
                <span aria-hidden="true">·</span>
              </Show>
              <span>{dimensions().cols} × {dimensions().rows}</span>
              <span aria-hidden="true">·</span><span>{renderer()}</span>
              <Show when={roundTripMs() != null}><span aria-hidden="true">·</span><span>{roundTripMs()} ms</span></Show>
              <Show when={!degraded()}><span aria-hidden="true">·</span><span>Persistent</span></Show>
              <span aria-hidden="true">·</span><span class="terminal-controls__shortcut" title="Move keyboard focus from terminal input to session tabs">Ctrl⇧F6 tools</span>
            </span>
          </div>
          <span class="sr-only" role="status" aria-live="polite">{statusText()}</span>
        </div>
      </div>
    </div>
  );
}
