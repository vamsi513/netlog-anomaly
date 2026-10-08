import { detectorLabel, type DetectorScore } from "@/lib/api";

// The API returns rows already sorted by F1, so the first row is the best.
export default function DetectorTable({ scores }: { scores: DetectorScore[] }) {
  return (
    <section className="panel">
      <h2>Detector comparison</h2>
      <table>
        <thead>
          <tr>
            <th>Detector</th>
            <th>Precision</th>
            <th>Recall</th>
            <th>F1</th>
            <th>TP</th>
            <th>FP</th>
            <th>FN</th>
            <th>TN</th>
          </tr>
        </thead>
        <tbody>
          {scores.map((score, index) => (
            <tr
              key={`${score.detector}-${score.variant}`}
              className={index === 0 ? "best" : undefined}
            >
              <td title={score.note}>{detectorLabel(score)}</td>
              <td>{score.precision.toFixed(4)}</td>
              <td>{score.recall.toFixed(4)}</td>
              <td>{score.f1.toFixed(4)}</td>
              <td>{score.true_positives}</td>
              <td>{score.false_positives}</td>
              <td>{score.false_negatives}</td>
              <td>{score.true_negatives}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="hint">
        Scored on the held-out test period only. Hover a detector for how it was
        fitted.
      </p>
    </section>
  );
}
