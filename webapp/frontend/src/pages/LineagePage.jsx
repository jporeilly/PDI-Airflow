import { useEffect, useState } from 'react'
import { apiGet } from './../api.js'

const STATE_BADGE = {
  COMPLETED: 'good', RUNNING: 'accent', FAILED: 'serious', ABORTED: 'warning',
}

// Marquez's graph only knows JOB and DATASET nodes; the PDI level
// travels in the OpenLineage jobType facet — surfaced here as a label.
const TYPE_LABEL = {
  STEP: ['step', 'neutral'],
  TRANS: ['transformation', 'accent'],
  JOB: ['job entry', 'accent'],
  DAG: ['airflow dag', 'good'],
  TASK: ['airflow task', 'neutral'],
}

// Marquez lineage summary for the pdi namespace, with a link out to the
// full Marquez UI for the graph view.
export default function LineagePage() {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    let stop = false
    const poll = () =>
      apiGet('/api/marquez/jobs')
        .then((d) => { if (!stop) { setData(d); setError('') } })
        .catch((e) => { if (!stop) setError(e.message) })
    poll()
    const t = setInterval(poll, 15000)
    return () => { stop = true; clearInterval(t) }
  }, [])

  return (
    <>
      <div className="page-head">
        <h1>Lineage in Marquez</h1>
        <p className="psub">
          Every DAG run emits OpenLineage events; Marquez stores the runs and
          renders the graph.
        </p>
      </div>

      <section className="card">
        <header>
          <h2>
            Jobs in namespace <span>{data?.namespace ?? '…'}</span>
          </h2>
        </header>
        {error && <div className="error">{error}</div>}
        {!data && !error && <div className="loading">Loading…</div>}
        {data?.jobs?.length === 0 && (
          <p className="hint-line">
            No lineage yet — trigger any DAG run and refresh.
          </p>
        )}
        {data?.jobs?.length > 0 && (
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>Job</th><th>Type</th><th>Last state</th><th>Duration</th><th>Updated</th></tr>
              </thead>
              <tbody>
                {data.jobs.map((j) => {
                  const [label, badge] = TYPE_LABEL[j.type] ?? [j.type?.toLowerCase() ?? '—', 'neutral']
                  return (
                  <tr key={j.name}>
                    <td>{j.name}</td>
                    <td><span className={`badge ${badge}`}>{label}</span></td>
                    <td>
                      {j.state
                        ? <span className={`badge ${STATE_BADGE[j.state] ?? 'neutral'}`}>{j.state}</span>
                        : <span className="badge neutral">no runs</span>}
                    </td>
                    <td>{j.duration_ms != null ? `${(j.duration_ms / 1000).toFixed(1)}s` : '—'}</td>
                    <td>{j.updated_at ? new Date(j.updated_at).toLocaleString() : '—'}</td>
                  </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
        {data?.marquez_url && (
          <div className="actions">
            <a href={data.marquez_url} target="_blank" rel="noreferrer">
              <button className="primary">Open the graph in Marquez →</button>
            </a>
          </div>
        )}
      </section>
    </>
  )
}
