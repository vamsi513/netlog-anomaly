// Typed client for the read-only netlog-anomaly API.
// Everything rendered by this app comes from these endpoints; there is no
// sample or placeholder data anywhere in the frontend.

export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export interface Summary {
  window_seconds: number;
  windows: number;
  anomalous_windows: number;
  first_window: string;
  last_window: string;
  events: number;
  first_scored: string | null;
  last_scored: string | null;
  scored_windows: number;
  first_anomalous_index: number | null;
}

export interface DetectorScore {
  detector: string;
  variant: string;
  note: string;
  precision: number;
  recall: number;
  f1: number;
  true_positives: number;
  false_positives: number;
  false_negatives: number;
  true_negatives: number;
}

export interface WindowRow {
  window_start: string;
  event_count: number;
  distinct_nodes: number;
  distinct_templates: number;
  error_rate: number;
  alert_count: number;
  is_anomalous: boolean;
  detector_flags: Record<string, boolean>;
}

export interface TemplateRow {
  template_id: string;
  template: string;
  event_count: number;
}

export interface WindowDetail {
  window: Omit<WindowRow, "detector_flags">;
  templates: TemplateRow[];
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export function fetchSummary(windowSeconds = 60): Promise<Summary> {
  return getJson<Summary>(`/api/summary?window_seconds=${windowSeconds}`);
}

export function fetchDetectors(windowSeconds = 60): Promise<DetectorScore[]> {
  return getJson<DetectorScore[]>(`/api/detectors?window_seconds=${windowSeconds}`);
}

export function fetchWindows(
  limit: number,
  offset: number,
  windowSeconds = 60,
): Promise<WindowRow[]> {
  const query = new URLSearchParams({
    window_seconds: String(windowSeconds),
    limit: String(limit),
    offset: String(offset),
    scored_only: "true",
  });
  return getJson<WindowRow[]>(`/api/windows?${query}`);
}

export function fetchWindowDetail(
  windowStart: string,
  windowSeconds = 60,
): Promise<WindowDetail> {
  const query = new URLSearchParams({
    window_seconds: String(windowSeconds),
    limit: "15",
  });
  return getJson<WindowDetail>(
    `/api/windows/${encodeURIComponent(windowStart)}/templates?${query}`,
  );
}

/**
 * Page offset that brings `index` into view, given a page size.
 *
 * The dashboard opens on the first anomalous scored window rather than at the
 * start of the test period, which on this dataset is a long quiet stretch. A
 * null or out-of-range index falls back to the first page.
 */
export function pageOffsetFor(
  index: number | null,
  pageSize: number,
  total: number,
): number {
  if (index === null || !Number.isFinite(index) || index < 0) return 0;
  if (total > 0 && index >= total) return 0;
  return Math.floor(index / pageSize) * pageSize;
}

// Formatting helpers shared by the components.

export function formatTimestamp(value: string): string {
  return value.replace("T", " ").replace("Z", "").replace("+00:00", "");
}

export function formatClockTime(value: string): string {
  return formatTimestamp(value).slice(5, 16);
}

export function formatNumber(value: number): string {
  return value.toLocaleString("en-US");
}

export function detectorLabel(score: DetectorScore): string {
  return score.variant === "-"
    ? score.detector
    : `${score.detector} (${score.variant})`;
}
