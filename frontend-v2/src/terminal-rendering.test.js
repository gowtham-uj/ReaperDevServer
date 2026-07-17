import test from "node:test";
import assert from "node:assert/strict";
import {
  joinTerminalPayloads,
  shouldRefitTerminal,
  terminalDimensionsDiffer,
  terminalGeometryOverflows,
  terminalResizeSettleDelay
} from "./terminal-rendering.js";

test("font metric drift requests a second terminal fit", () => {
  assert.equal(shouldRefitTerminal({
    current: { cols: 201, rows: 44 },
    proposed: { cols: 176, rows: 41 },
    hostRect: { width: 1440, height: 812 },
    screenRect: { width: 1608, height: 836 }
  }), true);

  assert.equal(shouldRefitTerminal({
    current: { cols: 176, rows: 41 },
    proposed: { cols: 176, rows: 41 },
    hostRect: { width: 1440, height: 812 },
    screenRect: { width: 1408, height: 779 }
  }), false);
});

test("geometry overflow is detected even before proposed dimensions change", () => {
  assert.equal(terminalDimensionsDiffer({ cols: 120, rows: 30 }, { cols: 120, rows: 30 }), false);
  assert.equal(terminalGeometryOverflows(
    { width: 1000, height: 600 },
    { width: 1100, height: 580 }
  ), true);
});

test("resize output waits for the remote pane to repaint", () => {
  assert.equal(terminalResizeSettleDelay(undefined), 160);
  assert.equal(terminalResizeSettleDelay(118), 198);
  assert.equal(terminalResizeSettleDelay(1_000), 500);
});

test("live write batching preserves split control sequences byte-for-byte", () => {
  const first = Uint8Array.from([0x1b, 0x5b, 0x32]);
  const second = Uint8Array.from([0x4a, 0xe2, 0x9c, 0x93]);
  const joined = joinTerminalPayloads([first, second]);
  assert.deepEqual([...joined], [0x1b, 0x5b, 0x32, 0x4a, 0xe2, 0x9c, 0x93]);
  assert.equal(joinTerminalPayloads([first]), first);
});
