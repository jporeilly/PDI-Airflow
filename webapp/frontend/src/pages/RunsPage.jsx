import { useState } from 'react'
import { apiPost } from './../api.js'
import { useStudio } from './../state.js'

// What each transformation actually did on its last Carte run. Without
// this you leave the app to find out whether the migration worked.
export default function RunsPage({ onNavigate }) {
  const ws = useStudio()
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [report, setReport] = useState(null)

  const files = ws.files.filter(
    (f) => f.doc && !f.filename.toLowerCase().endsWith('.kjb'))

  async function run() {
    setError(''); setBusy(true)
    try {
      setReport(await apiPost('/api/carte/runs', {
        files: files.map((f) => ({
          filename: f.filename, content: f.content,
          repo_path: f.repoPath || '',
        })),
        target: 'marquez',
      }))
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <h1>Carte runs</h1>
      <p className="psub">
        The most recent run of each loaded transformation, with the row
        counts each step moved — the same numbers the published lineage
        carries. Runs are picked by start time, never by their position
        in Carte&apos;s response, which has no guaranteed order.
      </p>

      <section className="card">
        <header><h2>Loaded transformations</h2></header>
        {files.length === 0 ? (
          <p className="hint-line">
            Load one or more <code>.ktr</code> files first.{' '}
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
              {busy ? 'Reading Carte…' : 'Fetch latest runs'}
            </button>
          </>
        )}
        {error && (
          <p className="hint-line">
            <span className="badge serious">error</span> {error}
          </p>
        )}
      </section>

      {report && report.runs.map((r) => (
        <section className="card" key={r.transformation}>
          <header>
            <h2>{r.transformation}</h2>
            <span className="psub mono">{r.repo_path}</span>
          </header>

          {r.error && (
            <p className="hint-line">
              <span className="badge serious">error</span> {r.error}
            </p>
          )}

          {!r.error && !r.run_id && (
            <p className="hint-line">
              <span className="badge neutral">never run</span> This
              transformation has not run on {report.carte_url}. Migrate
              and trigger it, or run it from Spoon, then check again.
            </p>
          )}

          {r.run_id && (
            <>
              <p className="hint-line">
                run <span className="mono">{r.run_id}</span>{' '}
                {r.errors > 0
                  ? <span className="badge serious">{r.errors} step error(s)</span>
                  : <span className="badge good">no errors</span>}
              </p>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Step</th><th>Type</th><th>Read</th>
                      <th>Written</th><th>Input</th><th>Output</th>
                      <th>Errors</th>
                    </tr>
                  </thead>
                  <tbody>
                    {r.steps.map((s) => (
                      <tr key={s.name}>
                        <td className="mono">{s.name}</td>
                        <td className="psub">{s.type}</td>
                        <td>{s.read}</td>
                        <td>{s.written}</td>
                        <td>{s.input}</td>
                        <td>{s.output}</td>
                        <td>
                          {s.errors > 0
                            ? <span className="badge serious">{s.errors}</span>
                            : 0}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {r.steps.length === 0 && (
                <p className="hint-line">
                  The run reported no step metrics.
                </p>
              )}
            </>
          )}
        </section>
      ))}
    </>
  )
}
