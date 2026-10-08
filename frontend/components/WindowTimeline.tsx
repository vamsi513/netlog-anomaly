"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  formatClockTime,
  formatNumber,
  formatTimestamp,
  type WindowRow,
} from "@/lib/api";

interface Props {
  windows: WindowRow[];
  selected: string | null;
  onSelect: (windowStart: string) => void;
}

// Bars are coloured by the dataset's own label, not by any detector output,
// so the chart shows ground truth and the drill-down shows what was predicted.
const NORMAL = "#4b89d4";
const ANOMALOUS = "#d9534f";

function TooltipBody({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { payload: WindowRow }[];
}) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="panel" style={{ margin: 0, padding: "10px 14px" }}>
      <div>{formatTimestamp(row.window_start)}</div>
      <div>{formatNumber(row.event_count)} events</div>
      <div>{row.is_anomalous ? "labelled anomalous" : "labelled normal"}</div>
    </div>
  );
}

export default function WindowTimeline({ windows, selected, onSelect }: Props) {
  return (
    <section className="panel">
      <h2>Window timeline</h2>
      <div className="legend">
        <span>
          <span className="swatch" style={{ background: NORMAL }} />
          labelled normal
        </span>
        <span>
          <span className="swatch" style={{ background: ANOMALOUS }} />
          labelled anomalous
        </span>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart
          data={windows}
          margin={{ top: 4, right: 8, bottom: 4, left: 8 }}
          onClick={(state) => {
            // recharts 3 reports the active tick index rather than the row,
            // and types it as number | string | null, so normalise it before
            // looking the window up in the data we passed in.
            const index = Number(state?.activeIndex);
            if (!Number.isInteger(index)) return;
            const row = windows[index];
            if (row) onSelect(row.window_start);
          }}
        >
          <CartesianGrid stroke="#272c36" vertical={false} />
          <XAxis
            dataKey="window_start"
            tickFormatter={formatClockTime}
            stroke="#9aa3b2"
            fontSize={11}
            minTickGap={48}
          />
          <YAxis
            // Event counts per window span one to tens of thousands, so a
            // linear axis renders every ordinary window as a flat line next to
            // a handful of spikes.
            scale="log"
            // The lower bound sits below one so that single-event windows,
            // which are the most common kind, still draw a visible bar instead
            // of collapsing onto the axis.
            domain={[0.7, "auto"]}
            allowDataOverflow
            stroke="#9aa3b2"
            fontSize={11}
            width={56}
            tickFormatter={formatNumber}
          />
          <Tooltip content={<TooltipBody />} cursor={{ fill: "#ffffff12" }} />
          <Bar dataKey="event_count" isAnimationActive={false}>
            {windows.map((row) => (
              <Cell
                key={row.window_start}
                fill={row.is_anomalous ? ANOMALOUS : NORMAL}
                stroke={row.window_start === selected ? "#ffffff" : undefined}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <p className="hint">
        Event count per window, log scale. Click a bar to see that
        window&apos;s templates.
      </p>
    </section>
  );
}
