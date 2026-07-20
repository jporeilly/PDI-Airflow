import { useStudio } from './../state.js'

// Show every generated DAG (accordion per file) with its warnings.
export default function PreviewPage({ onNavigate }) {
  const ws = useStudio()

  if (ws.results.length === 0) {
    return (
      <section className="card">
        <header><h2>Preview</h2></header>
        <p className="hint-line">Generate DAGs on the Configure page first.</p>
        <div className="actions">
          <button className="primary" onClick={() => onNavigate('configure')}>← Go to Configure</button>
        </div>
      </section>
    )
  }

  const totalWarnings = ws.results.reduce((n, r) => n + r.warnings.length, 0)

  function download(result) {
    const blob = new Blob([result.code], { type: 'text/x-python' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = `${result.dag_id}.py`
    a.click()
    URL.revokeObjectURL(a.href)
  }

  return (
    <>
      <div className="page-head">
        <h1>Preview {ws.results.length} DAG{ws.results.length > 1 ? 's' : ''}</h1>
        <p className="psub">
          {totalWarnings
            ? `${totalWarnings} migration warning${totalWarnings > 1 ? 's' : ''} to review before deploying.`
            : 'No migration warnings.'}
        </p>
      </div>

      {ws.results.map((r) => (
        <details className="card" key={r.dag_id} open={ws.results.length === 1}>
          <summary>
            <b>{r.dag_id}.py</b>{' '}
            <span className="muted">from {r.filename}</span>{' '}
            {r.warnings.length > 0
              ? <span className="badge warning">{r.warnings.length} warning{r.warnings.length > 1 ? 's' : ''}</span>
              : <span className="badge good">clean</span>}
          </summary>
          {r.warnings.length > 0 && (
            <ul className="notes">
              {r.warnings.map((w, i) => (
                <li key={i}><span className="badge warning">warn</span> {w}</li>
              ))}
            </ul>
          )}
          <div className="table-scroll" style={{ maxHeight: 420 }}>
            <pre style={{ margin: 0, padding: '12px 14px', fontSize: 12, lineHeight: 1.55, whiteSpace: 'pre' }}>
              <code>{r.code}</code>
            </pre>
          </div>
          <div className="actions">
            <button className="ghost" onClick={() => download(r)}>
              Download {r.dag_id}.py
            </button>
          </div>
        </details>
      ))}

      <div className="actions">
        <button className="ghost" onClick={() => onNavigate('configure')}>← Adjust options</button>
        <button className="primary" onClick={() => onNavigate('deploy')}>Continue to Deploy →</button>
      </div>
    </>
  )
}
