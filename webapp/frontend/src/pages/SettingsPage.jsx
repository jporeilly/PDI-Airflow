import { useEffect, useState } from 'react'
import { apiGet, apiPost } from './../api.js'

// Grouped so each service (Airflow, Marquez, PDC) reads as its own
// connection config; PDC and Airflow have a Test button.
const GROUPS = [
  { title: 'Deployment', fields: [
    ['dags_folder', 'Dags folder', 'C:\\PDI-Airflow\\workshop\\dags'],
  ] },
  { title: 'Apache Airflow', test: 'airflow', fields: [
    ['airflow_url', 'Airflow URL', 'http://localhost:8088'],
    ['airflow_user', 'Airflow user', 'admin'],
    ['airflow_password', 'Airflow password', ''],
  ] },
  { title: 'Marquez', test: 'marquez', fields: [
    ['marquez_url', 'Marquez API URL', 'http://localhost:6001'],
    ['marquez_web_url', 'Marquez UI URL', 'http://localhost:3000'],
    ['marquez_namespace', 'Marquez namespace', 'pdi'],
  ] },
  { title: 'Pentaho Data Catalog', test: 'pdc', fields: [
    ['pdc_url', 'PDC URL', 'https://pentaho.io'],
    ['pdc_user', 'PDC user', 'catalog.admin'],
    ['pdc_password', 'PDC password', ''],
    ['pdi_server', 'PDI Server name (ETL node)', 'pdi2dag'],
  ] },
]

export default function SettingsPage() {
  const [form, setForm] = useState(null)
  const [saved, setSaved] = useState(false)
  const [error, setError] = useState('')
  const [tests, setTests] = useState({})   // group -> {busy, result}
  const [browsing, setBrowsing] = useState(false)

  useEffect(() => {
    apiGet('/api/settings').then(setForm).catch((e) => setError(e.message))
  }, [])

  async function browseFolder(key) {
    setBrowsing(true)
    setError('')
    try {
      const r = await apiGet('/api/browse/folder')
      if (r.error) setError(r.error)
      else if (r.path) setForm((f) => ({ ...f, [key]: r.path }))
    } catch (e) {
      setError(e.message)
    } finally {
      setBrowsing(false)
    }
  }

  async function save() {
    setError('')
    setSaved(false)
    try {
      await apiPost('/api/settings', form)
      setSaved(true)
      setTimeout(() => setSaved(false), 2500)
    } catch (e) {
      setError(e.message)
    }
  }

  async function test(kind) {
    setTests((t) => ({ ...t, [kind]: { busy: true } }))
    // Save first so the probe uses the current field values.
    try { await apiPost('/api/settings', form) } catch { /* ignore */ }
    try {
      const s = await apiGet(`/api/${kind}/status`)
      let ok
      if (kind === 'pdc') {
        ok = s.authenticated ? 'connected' : s.reachable ? 'reachable (check credentials)' : 'offline'
      } else if (kind === 'marquez') {
        ok = s.reachable ? `connected${s.namespace_count != null ? ` · ${s.namespace_count} namespaces` : ''}` : 'offline'
      } else {
        ok = s.reachable ? `connected${s.dag_count != null ? ` · ${s.dag_count} DAGs` : ''}` : 'offline'
      }
      const good = kind === 'pdc' ? s.authenticated : s.reachable
      setTests((t) => ({ ...t, [kind]: { busy: false, good, text: ok } }))
    } catch (e) {
      setTests((t) => ({ ...t, [kind]: { busy: false, good: false, text: e.message } }))
    }
  }

  if (!form) return <div className="loading">Loading…</div>

  return (
    <>
      <div className="page-head">
        <h1>Settings</h1>
        <p className="psub">Where deployments go and which services the studio talks to.</p>
      </div>

      {GROUPS.map((g) => (
        <section className="card" key={g.title}>
          <header><h2>{g.title}</h2></header>
          <div className="form-grid">
            {g.fields.map(([key, label, placeholder]) => (
              <label key={key}>{label}
                {key === 'dags_folder' ? (
                  <span className="field-row">
                    <input
                      className="text"
                      type="text"
                      value={form[key] ?? ''}
                      placeholder={placeholder}
                      onChange={(e) => setForm({ ...form, [key]: e.target.value })}
                    />
                    <button type="button" className="ghost" disabled={browsing}
                      onClick={() => browseFolder(key)}>
                      {browsing ? 'Opening…' : 'Browse…'}
                    </button>
                  </span>
                ) : (
                  <input
                    className="text"
                    type={key.endsWith('password') ? 'password' : 'text'}
                    value={form[key] ?? ''}
                    placeholder={placeholder}
                    onChange={(e) => setForm({ ...form, [key]: e.target.value })}
                  />
                )}
              </label>
            ))}
          </div>
          {g.test && (
            <div className="actions">
              <button className="ghost" disabled={tests[g.test]?.busy}
                onClick={() => test(g.test)}>
                {tests[g.test]?.busy ? 'Testing…' : `Test ${g.title} connection`}
              </button>
              {tests[g.test]?.text && (
                <span className={`badge ${tests[g.test].good ? 'good' : 'warning'}`}>
                  {tests[g.test].text}
                </span>
              )}
            </div>
          )}
        </section>
      ))}

      <div className="actions">
        <button className="primary" onClick={save}>Save settings</button>
        {saved && <span className="badge good">saved</span>}
      </div>
      {error && <div className="error">{error}</div>}
    </>
  )
}
