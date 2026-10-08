// Run with: npm test
// Node's built-in test runner, with its native TypeScript type stripping, so
// this needs no test framework or transform of its own.
import assert from "node:assert/strict";
import { test } from "node:test";
import { detectorLabel, formatClockTime, formatTimestamp, pageOffsetFor } from "./api.ts";

const PAGE = 300;

test("opens on the page holding the first anomalous window", () => {
  // The real dataset's first anomalous scored window is at index 1117, which
  // belongs to the page starting at 900.
  assert.equal(pageOffsetFor(1117, PAGE, 8245), 900);
});

test("an index on a page boundary opens that page, not the one before", () => {
  assert.equal(pageOffsetFor(900, PAGE, 8245), 900);
  assert.equal(pageOffsetFor(899, PAGE, 8245), 600);
});

test("an early anomalous window still opens the first page", () => {
  assert.equal(pageOffsetFor(0, PAGE, 8245), 0);
  assert.equal(pageOffsetFor(12, PAGE, 8245), 0);
});

test("no anomalous scored window falls back to the first page", () => {
  assert.equal(pageOffsetFor(null, PAGE, 8245), 0);
});

test("an out of range index falls back rather than paging past the end", () => {
  assert.equal(pageOffsetFor(9000, PAGE, 8245), 0);
  assert.equal(pageOffsetFor(-5, PAGE, 8245), 0);
  assert.equal(pageOffsetFor(Number.NaN, PAGE, 8245), 0);
});

test("a zero total does not reject an index", () => {
  // scored_windows can legitimately be unknown to the caller; only a known
  // total is used to bound the index.
  assert.equal(pageOffsetFor(1117, PAGE, 0), 900);
});

test("the chosen offset always lands on a page boundary", () => {
  for (const index of [0, 1, 299, 300, 301, 1117, 8244]) {
    assert.equal(pageOffsetFor(index, PAGE, 8245) % PAGE, 0);
  }
});

test("the chosen offset never skips past the target index", () => {
  for (const index of [0, 299, 300, 1117, 8244]) {
    const offset = pageOffsetFor(index, PAGE, 8245);
    assert.ok(offset <= index, `${offset} > ${index}`);
    assert.ok(index < offset + PAGE, `${index} not within page at ${offset}`);
  }
});

test("timestamps render without the T and Z", () => {
  assert.equal(formatTimestamp("2005-11-03T12:36:00Z"), "2005-11-03 12:36:00");
  assert.equal(formatClockTime("2005-11-03T12:36:00Z"), "11-03 12:36");
});

test("a detector with no variant is labelled by name alone", () => {
  const base = {
    note: "",
    precision: 0,
    recall: 0,
    f1: 0,
    true_positives: 0,
    false_positives: 0,
    false_negatives: 0,
    true_negatives: 0,
  };
  assert.equal(
    detectorLabel({ ...base, detector: "severity rule", variant: "-" }),
    "severity rule",
  );
  assert.equal(
    detectorLabel({ ...base, detector: "gradient boosting", variant: "severity-free" }),
    "gradient boosting (severity-free)",
  );
});
