import { useEffect, useState } from 'react'
import { apiGet, apiPost, runJob } from './../api.js'
import { setDeployed, setLineage, useStudio } from './../state.js'

// Deploy the whole batch: every generated DAG runs as its own backend
// migrate job, in parallel — they all wait on the same scheduler scan,
// so a batch costs one scan cycle, not one per DAG. Afterwards the PDI
// structure (jobs + steps) can be published to Marquez in one click.
export default function DeployPage({ onNavigate }) {
  const ws = useStudio()
  const [settings, setSettings] = useState(null)
  const [activate, setActivate] = useState(true)
  const [trigger, setTrigger] = useState(false)
  const [jobs, setJobs] = useState({})       // dag_id -> job dict
  const [error, setError] = useState('')
  const [running, setRunning] = useState(false)
  const [lineageBusy, setLineageBusy] = useState(false)
  const [lineageError, setLineageError] = useState('')

  const [preflight, setPreflight] = useState(null)

  useEffect(() => {
    apiGet('/api/settings').then(setSettings).catch(() => {})
    apiGet('/api/deploy/preflight').then(setPreflight).catch(() => {})
  }, [])

  const hasFiles = ws.files.some((f) => f.doc)

  if (!hasFiles) {
    return (
      <section className="card">
        <header><h2>Deploy</h2></header>
        <p className="hint-line">Load a PDI file first.</p>
        <div className="actions">
          <button className="primary" onClick={() => onNavigate('load')}>← Go to Load</button>
        </div>
      </section>
    )
  }

  async function deployAll() {
    setError('')
    setDeployed(null)
    setJobs({})
    setRunning(true)
    const outcomes = await Promise.all(ws.results.map((r) =>
      runJob('migrate', {
        dag_id: r.dag_id,
        code: r.code,
        activate,
        trigger,
        schedule: ws.options.schedule,
      }, (job) => setJobs((prev) => ({ ...prev, [r.dag_id]: job })))
        .then((job) => ({ ...job.result }))
        .catch((err) => ({ dag_id: r.dag_id, error: err.message }))))
    setRunning(false)
    setDeployed({ items: outcomes })
    if (outcomes.some((o) => o.error)) {
      setError('Some DAGs failed to deploy — see the table below.')
    }
  }

  async function publishLineage(target) {
    setLineageError('')
    setLineageBusy(true)
    try {
      const result = await apiPost('/api/lineage/publish', {
        target,
        files: ws.files
          .filter((f) => f.doc)
          .map((f) => ({ filename: f.filename, content: f.content })),
      })
      if (target === 'file' && result.ndjson) {
        const blob = new Blob([result.ndjson], { type: 'application/json' })
        const a = document.createElement('a')
        a.href = URL.createObjectURL(blob)
        a.download = 'pdi.openlineage.json'
        a.click()
        URL.revokeObjectURL(a.href)
      }
      setLineage(result)
    } catch (err) {
      setLineageError(err.message)
    } finally {
      setLineageBusy(false)
    }
  }

  const n = ws.results.length
  const hasResults = n > 0
  return (
    <>
      <div className="page-head">
        <h1>Deploy{hasResults ? ` ${n} DAG${n > 1 ? 's' : ''}` : ''}</h1>
        <p className="psub">
          Target: {settings ? `${settings.dags_folder} → ${settings.airflow_url}` : '…'}
          {'  '}(change under Settings)
        </p>
        {preflight && !preflight.delivers && (
          <p className="hint-line">
            <span className="badge warning">writes locally</span>{' '}
            {preflight.note}
          </p>
        )}
        {preflight && preflight.delivers && !preflight.folder_exists && (
          <p className="hint-line">
            <span className="badge warning">missing folder</span>{' '}
            <span className="mono">{preflight.dags_folder}</span> does
            not exist yet — it will be created on deploy.
          </p>
        )}
        <p className="psub" style={{ display: 'none' }}>
        </p>
      </div>

      {!hasResults && (
        <section className="card">
          <header><h2>Deploy to Airflow</h2></header>
          <p className="hint-line">
            Generate DAGs on the Configure page to deploy them to Airflow.
            Publishing PDI lineage below only needs loaded files.
          </p>
          <div className="actions">
            <button className="ghost" onClick={() => onNavigate('configure')}>← Go to Configure</button>
          </div>
        </section>
      )}

      {hasResults && (
      <section className="card">
        <header><h2>Hand-over options</h2></header>
        <div className="form-grid">
          <label>Activate (unpause) after parse
            <select value={activate ? 'yes' : 'no'} onChange={(e) => setActivate(e.target.value === 'yes')}>
              <option value="yes">yes — schedules go live</option>
              <option value="no">no — deploy paused</option>
            </select>
          </label>
          <label>Trigger a first run now
            <select value={trigger ? 'yes' : 'no'} onChange={(e) => setTrigger(e.target.value === 'yes')}>
              <option value="no">no</option>
              <option value="yes">yes</option>
            </select>
          </label>
        </div>
        <div className="actions">
          <button className="primary" disabled={running} onClick={deployAll}>
            {running ? 'Deploying…' : `Deploy ${n} DAG${n > 1 ? 's' : ''} to Airflow`}
          </button>
        </div>

        {Object.keys(jobs).length > 0 && (
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>DAG</th><th>Status</th><th>Phase</th></tr>
              </thead>
              <tbody>
                {ws.results.map((r) => {
                  const job = jobs[r.dag_id]
                  const item = ws.deployed?.items?.find((i) => i.dag_id === r.dag_id)
                  return (
                    <tr key={r.dag_id}>
                      <td>{r.dag_id}</td>
                      <td>
                        {item?.error
                          ? <span className="badge serious">failed</span>
                          : item
                            ? <span className="badge good">deployed</span>
                            : job
                              ? <span className="badge accent">{job.status}</span>
                              : <span className="badge neutral">queued</span>}
                      </td>
                      <td className="hint-line">
                        {item?.error || item?.dag_file || job?.phase || ''}
                        {item?.run_id ? ` — run ${item.run_id}` : ''}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
        {error && <div className="error">{error}</div>}
      </section>
      )}

      <section className="card">
        <header>
          <h2>PDI structure → Marquez / PDC <span>jobs, transformations and steps</span></h2>
        </header>
        <p className="hint-line">
          Publishes the loaded files' structure as OpenLineage events:
          job entries linked by hop-derived datasets, plus the step graph
          of every dropped transformation. PDC ingests them on the same
          endpoint the official PDI OpenLineage plugin uses
          (/lineage/api/events → ETL Pipelines).
        </p>
        <div className="actions">
          <button className="primary" disabled={lineageBusy}
            onClick={() => publishLineage('marquez')}>
            {lineageBusy ? 'Publishing…' : 'Publish to Marquez'}
          </button>
          <button className="primary" disabled={lineageBusy}
            onClick={() => publishLineage('pdc')}>
            {lineageBusy ? 'Publishing…' : 'Publish to PDC'}
          </button>
          <button className="ghost" disabled={lineageBusy}
            onClick={() => publishLineage('file')}
            title="Download openlineage.json for PDC ETL → Actions → Import">
            Download for PDC Import
          </button>
          {ws.lineage && (
            <span className="summary">
              <span className="badge good">ok</span>{' '}
              {ws.lineage.events} events · {ws.lineage.jobs} PDI jobs
              {ws.lineage.steps ? ` · ${ws.lineage.steps} steps` : ''} →{' '}
              {ws.lineage.target === 'pdc'
                ? <a href={settings?.pdc_url} target="_blank" rel="noreferrer">open PDC</a>
                : <a href={settings?.marquez_web_url} target="_blank" rel="noreferrer">open Marquez</a>}
            </span>
          )}
        </div>
        {lineageError && <div className="error">{lineageError}</div>}
      </section>

      {ws.deployed && !error && (
        <div className="actions">
          {settings && (
            <a href={settings.airflow_url} target="_blank" rel="noreferrer">
              <button className="ghost">Open Airflow →</button>
            </a>
          )}
          <button className="ghost" onClick={() => onNavigate('lineage')}>View lineage →</button>
        </div>
      )}
    </>
  )
}
