"use client";

import { useCallback, useEffect, useState } from "react";
import DetectorTable from "@/components/DetectorTable";
import WindowDetailPanel from "@/components/WindowDetailPanel";
import WindowTimeline from "@/components/WindowTimeline";
import {
  fetchDetectors,
  fetchSummary,
  fetchWindowDetail,
  fetchWindows,
  formatNumber,
  formatTimestamp,
  type DetectorScore,
  type Summary,
  type WindowDetail,
  type WindowRow,
} from "@/lib/api";

// The scored period runs to thousands of windows, so the timeline pages
// through them rather than trying to draw them all at once.
const PAGE_SIZE = 300;

export default function Page() {
  const [summary, setSummary] = useState<Summary | null>(null);
  const [detectors, setDetectors] = useState<DetectorScore[]>([]);
  const [windows, setWindows] = useState<WindowRow[]>([]);
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<WindowDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([fetchSummary(), fetchDetectors()])
      .then(([nextSummary, nextDetectors]) => {
        setSummary(nextSummary);
        setDetectors(nextDetectors);
      })
      .catch((cause: Error) => setError(cause.message));
  }, []);

  useEffect(() => {
    fetchWindows(PAGE_SIZE, offset)
      .then(setWindows)
      .catch((cause: Error) => setError(cause.message));
  }, [offset]);

  const selectWindow = useCallback((windowStart: string) => {
    setSelected(windowStart);
    setLoadingDetail(true);
    fetchWindowDetail(windowStart)
      .then(setDetail)
      .catch((cause: Error) => setError(cause.message))
      .finally(() => setLoadingDetail(false));
  }, []);

  if (error) {
    return (
      <main>
        <h1>netlog-anomaly</h1>
        <section className="panel error">
          <h2>Could not load data</h2>
          <p>{error}</p>
          <p className="hint">
            Start the API with{" "}
            <code>uvicorn netlog_anomaly.api:app</code> and make sure the
            pipeline has been run.
          </p>
        </section>
      </main>
    );
  }

  const scoredTotal = summary?.scored_windows ?? 0;
  const shownFrom = windows.length === 0 ? 0 : offset + 1;
  const shownTo = offset + windows.length;

  return (
    <main>
      <h1>netlog-anomaly</h1>
      <p className="subtitle">
        BGL log dataset, {summary ? `${summary.window_seconds}s` : "—"} windows.
        Everything here is read from Postgres.
      </p>

      {summary && (
        <div className="stats">
          <Stat label="Events" value={formatNumber(summary.events)} />
          <Stat label="Windows" value={formatNumber(summary.windows)} />
          <Stat
            label="Anomalous"
            value={`${formatNumber(summary.anomalous_windows)} (${(
              (summary.anomalous_windows / summary.windows) *
              100
            ).toFixed(2)}%)`}
          />
          <Stat label="Scored windows" value={formatNumber(scoredTotal)} />
          <Stat
            label="Covered"
            value={`${formatTimestamp(summary.first_window).slice(0, 10)} → ${formatTimestamp(
              summary.last_window,
            ).slice(0, 10)}`}
          />
        </div>
      )}

      <WindowTimeline
        windows={windows}
        selected={selected}
        onSelect={selectWindow}
      />

      <div className="controls">
        <button
          // Functional updates: two clicks before a re-render would otherwise
          // both read the same stale offset and only move one page.
          onClick={() => setOffset((current) => Math.max(0, current - PAGE_SIZE))}
          disabled={offset === 0}
        >
          ← Earlier
        </button>
        <button
          onClick={() => setOffset((current) => current + PAGE_SIZE)}
          disabled={shownTo >= scoredTotal}
        >
          Later →
        </button>
        <span>
          Showing scored windows {formatNumber(shownFrom)}–{formatNumber(shownTo)} of{" "}
          {formatNumber(scoredTotal)}
        </span>
      </div>

      <div style={{ height: 24 }} />

      <DetectorTable scores={detectors} />

      <WindowDetailPanel
        detail={detail}
        flags={windows.find((row) => row.window_start === selected)?.detector_flags}
        loading={loadingDetail}
      />
    </main>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
    </div>
  );
}
