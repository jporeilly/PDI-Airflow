import { useState } from 'react'
import { apiPost } from './../api.js'
import { useStudio } from './../state.js'

// Two independent measurements of the same table — what PDC profiled
// and what the pipeline actually read — side by side. Neither PDC's nor
// Marquez's UI shows them together, so the check lives here.
export default function ReconcilePage({ onNavigate }) {
  const ws = useStudio()
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [report, setReport] = useState(null)

  const files = ws.files.filter(
    (f) => f.doc && !f.filename.toLowerCase().endsWith('.kjb'))

  async function run() {
    setError(''); setBusy(true)
    try {
      setReport(await apiPost('/api/reconcile', {
        files: files.map((f) => ({
          filename: f.filename, content: f.content,
          repo_path: f.repoPath || '',
        })),
        target: 'pdc',
      }))
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const badge = { match: 'good', mismatch: 'serious', unknown: 'neutral' }

  return (
    <>
      <h1>Reconcile row counts</h1>
      <p className="psub">
        PDC profiles a table and records how many rows it holds. Carte
        reports how many it actually read. Agreement is a real
        data-quality signal — a mismatch means a filtered query, a
        partial load or a stale profile. A missing measurement is
        reported as <b>unknown</b>, never scored as a match.
      </p>

      <section className="card">
        <header><h2>Loaded transformations</h2></header>
        {files.length === 0 ? (
          <p className="hint-line">
            Load one or more <code>.ktr</code> files first — this compares
            their <i>input</i> tables.{' '}
            <button className="ghost" onClick={() => onNavigate('load')}>
              Go to Load →
            </button>
          </p>
        ) : (
          <>
            <p className="hint-line">
              {files.map((f) => f.doc.name).join(', ')}
            </p>
            <button onClick={run} disabled={busy}>
              {busy ? 'Comparing…' : `Reconcile ${files.length} transformation${files.length > 1 ? 's' : ''}`}
            </button>
          </>
        )}
        {error && <p className="hint-line"><span className="badge serious">error</span> {error}</p>}
      </section>

      {report && (
        <section className="card">
          <header>
            <h2>Result</h2>
            <span className="psub">
              {report.pdc_tables_profiled} table(s) profiled in PDC
            </span>
          </header>
          {report.results.map((r) => (
            <div key={r.transformation} style={{ marginBottom: 18 }}>
              <p>
                <b>{r.transformation}</b>{' '}
                <span className="mono psub">{r.repo_path}</span>{' '}
                {!r.ran_on_carte &&
                  <span className="badge warning">never run on Carte</span>}
              </p>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Dataset</th><th>PDC profiled</th>
                      <th>Pipeline read</th><th>Result</th>
                    </tr>
                  </thead>
                  <tbody>
                    {r.datasets.map((d) => (
                      <tr key={d.dataset}>
                        <td>
                          <span className="mono">{d.dataset}</span>
                          <br />
                          <span className="psub mono">{d.namespace}</span>
                        </td>
                        <td>{d.pdc_rows ?? '—'}</td>
                        <td>{d.carte_rows ?? '—'}</td>
                        <td>
                          <span className={`badge ${badge[d.status]}`}>
                            {d.status}
                          </span>
                          <br />
                          <span className="hint-line">{d.detail}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </section>
      )}
    </>
  )
}
