import {
  formatNumber,
  formatTimestamp,
  type WindowDetail,
  type WindowRow,
} from "@/lib/api";

interface Props {
  detail: WindowDetail | null;
  flags: Record<string, boolean> | undefined;
  loading: boolean;
}

export default function WindowDetailPanel({ detail, flags, loading }: Props) {
  if (loading) {
    return (
      <section className="panel">
        <h2>Window detail</h2>
        <p className="hint">Loading…</p>
      </section>
    );
  }

  if (!detail) {
    return (
      <section className="panel">
        <h2>Window detail</h2>
        <p className="hint">Select a window in the timeline above.</p>
      </section>
    );
  }

  const { window: row, templates } = detail;
  const flagEntries = Object.entries(flags ?? {});

  return (
    <section className="panel">
      <h2>Window {formatTimestamp(row.window_start)}</h2>
      <div className="stats">
        <Stat label="Events" value={formatNumber(row.event_count)} />
        <Stat label="Nodes" value={formatNumber(row.distinct_nodes)} />
        <Stat label="Templates" value={formatNumber(row.distinct_templates)} />
        <Stat label="Error rate" value={row.error_rate.toFixed(4)} />
        <Stat label="Alert lines" value={formatNumber(row.alert_count)} />
        <Stat label="Label" value={row.is_anomalous ? "anomalous" : "normal"} />
      </div>

      <h2>Detector flags</h2>
      {flagEntries.length === 0 ? (
        <p className="hint">
          This window is outside the scored test period, so no detector ran on
          it.
        </p>
      ) : (
        <ul className="flags">
          {flagEntries.map(([name, flagged]) => (
            <li key={name} className={flagged ? "flag flag-on" : "flag"}>
              {name}: {flagged ? "flagged" : "clear"}
            </li>
          ))}
        </ul>
      )}

      <h2 style={{ marginTop: 22 }}>Top templates</h2>
      <table>
        <thead>
          <tr>
            <th>Template</th>
            <th>Count</th>
          </tr>
        </thead>
        <tbody>
          {templates.map((template) => (
            <tr key={template.template_id}>
              <td className="template">{template.template || "(empty)"}</td>
              <td>{formatNumber(template.event_count)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
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

export type { WindowRow };
