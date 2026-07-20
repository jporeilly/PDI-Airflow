import { useEffect, useState } from 'react'
import { apiGet, apiPost } from './../api.js'
import {
  rootFiles, setOptions, setParam, setResults, useStudio,
} from './../state.js'

const PRESETS = [
  { label: 'Manual only', value: '' },
  { label: 'Hourly', value: '0 * * * *' },
  { label: 'Daily 06:00', value: '0 6 * * *' },
  { label: 'Weekdays 05:00', value: '0 5 * * 1-5' },
  { label: 'Weekly Sun 07:00', value: '0 7 * * 0' },
]

// Shared schedule + conversion options for the whole batch; generates
// one DAG per root file on continue.
export default function ConfigurePage({ onNavigate }) {
  const ws = useStudio()
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [conns, setConns] = useState(null)   // pentaho connections in Airflow
  const o = ws.options
  const roots = rootFiles(ws.files)

  useEffect(() => {
    apiGet('/api/airflow/connections')
      .then((r) => setConns(r.connections || []))
      .catch(() => setConns([]))
  }, [])

  if (roots.length === 0) {
    return (
      <section className="card">
        <header><h2>Configure</h2></header>
        <p className="hint-line">Load at least one PDI file first.</p>
        <div className="actions">
          <button className="primary" onClick={() => onNavigate('load')}>← Go to Load</button>
        </div>
      </section>
    )
  }

  async function generate() {
    setError('')
    setBusy(true)
    try {
      const results = []
      for (const f of roots) {
        const result = await apiPost('/api/convert', {
          filename: f.filename,
          content: f.content,
          options: o,
        })
        results.push({ filename: f.filename, ...result })
      }
      setResults(results)
      onNavigate('preview')
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const hasJob = roots.some((f) => f.doc.kind === 'job')
  return (
    <>
      <div className="page-head">
        <h1>Configure the DAG{roots.length > 1 ? 's' : ''}</h1>
        <p className="psub">
          Options apply to every generated DAG:
          {' '}{roots.map((f) => f.doc.name).join(', ')}.
        </p>
      </div>

      <section className="card">
        <header><h2>Schedule</h2></header>
        <div className="form-grid">
          <label>Preset
            <select value={PRESETS.some((p) => p.value === o.schedule) ? o.schedule : 'custom'}
              onChange={(e) => e.target.value !== 'custom' && setOptions({ schedule: e.target.value })}>
              {PRESETS.map((p) => <option key={p.label} value={p.value}>{p.label}</option>)}
              <option value="custom">Custom cron…</option>
            </select>
          </label>
          <label>Cron expression
            <input className="text" value={o.schedule} placeholder="e.g. 30 6 * * *"
              onChange={(e) => setOptions({ schedule: e.target.value })} />
          </label>
        </div>
        <p className="hint-line">
          Empty schedule = manual-trigger-only DAGs. DAG ids default to
          the PDI job/transformation names.
        </p>
      </section>

      <section className="card">
        <header><h2>Conversion</h2></header>
        <div className="form-grid">
          <label>Mode
            <select value={o.mode} onChange={(e) => setOptions({ mode: e.target.value })} disabled={!hasJob}>
              <option value="auto">auto (recommended)</option>
              <option value="explode">explode — task per entry</option>
              <option value="wrap">wrap — single Carte task</option>
            </select>
          </label>
          <label>Carte connection
            <input className="text" list="pdi-conns" value={o.conn_id}
              placeholder="pdi_default"
              onChange={(e) => setOptions({ conn_id: e.target.value })} />
            <datalist id="pdi-conns">
              {(conns || []).map((c) => (
                <option key={c.conn_id} value={c.conn_id}>
                  {c.host ? `${c.host}${c.port ? ':' + c.port : ''}`
                    : c.source === 'env' ? 'env-var default' : ''}
                </option>
              ))}
            </datalist>
            <span className="field-note">
              {conns === null ? 'Loading Airflow connections…'
                : `${conns.length} Carte connection${conns.length > 1 ? 's' : ''} — pick one or type a name. `
                  + 'DB connections list here; env-var ones (like the lab’s pdi_default) work but don’t enumerate.'}
            </span>
          </label>
          <label>Log level
            <select value={o.level} onChange={(e) => setOptions({ level: e.target.value })}>
              {['Basic', 'Detailed', 'Debug', 'Rowlevel', 'Error', 'Nothing'].map((l) =>
                <option key={l}>{l}</option>)}
            </select>
          </label>
          <label>Retries
            <input className="text" type="number" min="0" value={o.retries}
              onChange={(e) => setOptions({ retries: Number(e.target.value) })} />
          </label>
          <label>Poll interval (s)
            <input className="text" type="number" min="1" value={o.poll_interval}
              onChange={(e) => setOptions({ poll_interval: Number(e.target.value) })} />
          </label>
          <label>Deferrable
            <select value={o.deferrable ? 'yes' : 'no'}
              onChange={(e) => setOptions({ deferrable: e.target.value === 'yes' })}>
              <option value="yes">yes — free the worker slot</option>
              <option value="no">no — classic polling</option>
            </select>
          </label>
        </div>
      </section>

      <section className="card">
        <header><h2>PDI parameters <span>union of all files — Airflow macros allowed, e.g. {'{{ ds }}'}</span></h2></header>
        {Object.keys(o.params).length === 0 && (
          <p className="hint-line">No named parameters declared. Add one below if needed.</p>
        )}
        <div className="form-grid">
          {Object.entries(o.params).map(([k, v]) => (
            <label key={k}>{k}
              <input className="text" value={v} onChange={(e) => setParam(k, e.target.value)} />
            </label>
          ))}
        </div>
        <AddParam />
      </section>

      {error && <div className="error">{error}</div>}
      <div className="actions">
        <button className="primary" disabled={busy} onClick={generate}>
          {busy ? 'Generating…' : `Generate ${roots.length} DAG${roots.length > 1 ? 's' : ''} →`}
        </button>
      </div>
    </>
  )
}

function AddParam() {
  const [name, setName] = useState('')
  return (
    <div className="actions">
      <input className="text" placeholder="new parameter name" value={name}
        onChange={(e) => setName(e.target.value)} />
      <button className="ghost" disabled={!name.trim()}
        onClick={() => { setParam(name.trim(), ''); setName('') }}>
        Add parameter
      </button>
    </div>
  )
}
