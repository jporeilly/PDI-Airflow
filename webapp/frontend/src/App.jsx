import { useEffect, useState } from 'react'
import ThemeSelect from './components/ThemeSelect.jsx'
import HomePage from './pages/HomePage.jsx'
import LoadPage from './pages/LoadPage.jsx'
import ConfigurePage from './pages/ConfigurePage.jsx'
import PreviewPage from './pages/PreviewPage.jsx'
import DeployPage from './pages/DeployPage.jsx'
import LineagePage from './pages/LineagePage.jsx'
import ReconcilePage from './pages/ReconcilePage.jsx'
import PdiGraphPage from './pages/PdiGraphPage.jsx'
import SettingsPage from './pages/SettingsPage.jsx'
import { useStudio } from './state.js'
import { apiGet } from './api.js'

/* Nav icons — the suite's shared visual family (24 viewBox, 1.7 stroke). */
const ICONS = {
  home: <path d="M4 13h6V4H4v9Zm0 7h6v-5H4v5Zm10 0h6V11h-6v9Zm0-16v5h6V4h-6Z" stroke="currentColor" strokeWidth="1.7" fill="none" />,
  load: <><path d="M13.5 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8.5L13.5 3Z" stroke="currentColor" strokeWidth="1.7" strokeLinejoin="round" fill="none" /><path d="M13.5 3v5.5H19" stroke="currentColor" strokeWidth="1.7" strokeLinejoin="round" fill="none" /></>,
  configure: <><circle cx="12" cy="12" r="3" stroke="currentColor" strokeWidth="1.7" fill="none" /><path d="M12 2v3m0 14v3M2 12h3m14 0h3M4.9 4.9l2.1 2.1m10 10 2.1 2.1M19.1 4.9 17 7m-10 10-2.1 2.1" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" fill="none" /></>,
  preview: <><rect x="4" y="4.5" width="16" height="15" rx="2" stroke="currentColor" strokeWidth="1.7" fill="none" /><path d="m8.5 9.5 3 2.5-3 2.5M13.5 15H16" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" fill="none" /></>,
  deploy: <><path d="M12 17V5m0 0-4.2 4.2M12 5l4.2 4.2" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" fill="none" /><path d="M4.5 19.5h15" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" fill="none" /></>,
  lineage: <><circle cx="5.5" cy="12" r="2.5" stroke="currentColor" strokeWidth="1.7" fill="none" /><circle cx="18.5" cy="5.5" r="2.5" stroke="currentColor" strokeWidth="1.7" fill="none" /><circle cx="18.5" cy="18.5" r="2.5" stroke="currentColor" strokeWidth="1.7" fill="none" /><path d="M7.8 10.9 16.2 6.6M7.8 13.1l8.4 4.3" stroke="currentColor" strokeWidth="1.7" fill="none" /></>,
  pdigraph: <><rect x="3.5" y="3.5" width="17" height="17" rx="2" stroke="currentColor" strokeWidth="1.7" fill="none" /><rect x="7" y="8" width="10" height="4" rx="1" stroke="currentColor" strokeWidth="1.5" fill="none" /><path d="M12 12v2.5" stroke="currentColor" strokeWidth="1.5" fill="none" /><rect x="9" y="14.5" width="6" height="3" rx="1" stroke="currentColor" strokeWidth="1.5" fill="none" /></>,
  settings: <><circle cx="12" cy="12" r="3" stroke="currentColor" strokeWidth="1.7" fill="none" /><path d="M12 2v3m0 14v3M2 12h3m14 0h3" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" fill="none" /></>,
}

function Ico({ id }) {
  return <svg className="nav-ico" viewBox="0 0 24 24">{ICONS[id]}</svg>
}

// The migration workflow stepper, shown on the four workflow pages.
const STEPS = [
  { id: 'load', label: 'Load', hint: 'pick a .kjb / .ktr' },
  { id: 'configure', label: 'Configure', hint: 'schedule & options' },
  { id: 'preview', label: 'Preview', hint: 'review generated DAG' },
  { id: 'deploy', label: 'Deploy', hint: 'hand over to Airflow' },
]

const PAGES = {
  home: HomePage,
  load: LoadPage,
  configure: ConfigurePage,
  preview: PreviewPage,
  deploy: DeployPage,
  pdigraph: PdiGraphPage,
  lineage: LineagePage,
  reconcile: ReconcilePage,
  settings: SettingsPage,
}

const CRUMBS = {
  home: ['Home'],
  load: ['Migrate', 'Load'],
  configure: ['Migrate', 'Configure'],
  preview: ['Migrate', 'Preview'],
  deploy: ['Migrate', 'Deploy'],
  pdigraph: ['Observe', 'PDI Graph'],
  lineage: ['Observe', 'Lineage'],
  reconcile: ['Observe', 'Reconcile'],
  settings: ['Configure', 'Settings'],
}

function Stepper({ page, onNavigate }) {
  const ws = useStudio()
  const hasDocs = ws.files.some((f) => f.doc)
  const done = {
    load: hasDocs,
    configure: ws.results.length > 0,
    preview: ws.results.length > 0,
    deploy: !!ws.deployed,
  }
  const unlocked = {
    load: true,
    configure: hasDocs,
    preview: hasDocs,
    deploy: ws.results.length > 0,
  }
  return (
    <ol className="stepper">
      {STEPS.map((s, i) => {
        const cls = [
          page === s.id ? 'active' : '',
          done[s.id] && page !== s.id ? 'done' : '',
          unlocked[s.id] ? '' : 'locked',
        ].filter(Boolean).join(' ')
        return (
          <li key={s.id} className={cls}>
            <button disabled={!unlocked[s.id]} onClick={() => onNavigate(s.id)}>
              <span className="dot">{i + 1}</span>
              <span className="step-text">
                <span className="step-label">{s.label}</span>
                <span className="step-hint">{s.hint}</span>
              </span>
            </button>
            {i < STEPS.length - 1 && <span className="bar" />}
          </li>
        )
      })}
    </ol>
  )
}

export default function App() {
  const [page, setPage] = useState('home')
  const [version, setVersion] = useState('')
  const [airflow, setAirflow] = useState(null)
  const [carte, setCarte] = useState(null)
  const [marquez, setMarquez] = useState(null)
  const [pdc, setPdc] = useState(null)
  const ws = useStudio()

  useEffect(() => {
    apiGet('/api/version')
      .then((v) => setVersion(v.version))
      .catch(() => {})
  }, [])

  useEffect(() => {
    let stop = false
    const poll = () => {
      apiGet('/api/airflow/status')
        .then((s) => { if (!stop) setAirflow(s) })
        .catch(() => { if (!stop) setAirflow(null) })
      apiGet('/api/carte/status')
        .then((s) => { if (!stop) setCarte(s) })
        .catch(() => { if (!stop) setCarte(null) })
      apiGet('/api/marquez/status')
        .then((s) => { if (!stop) setMarquez(s) })
        .catch(() => { if (!stop) setMarquez(null) })
      apiGet('/api/pdc/status')
        .then((s) => { if (!stop) setPdc(s) })
        .catch(() => { if (!stop) setPdc(null) })
    }
    poll()
    const t = setInterval(poll, 60000)
    return () => { stop = true; clearInterval(t) }
  }, [])

  const Page = PAGES[page]
  const crumbs = CRUMBS[page]
  const inWorkflow = ['load', 'configure', 'preview', 'deploy'].includes(page)

  const navItem = (id, label) => (
    <button
      className={`nav-item${page === id ? ' active' : ''}`}
      onClick={() => setPage(id)}
    >
      <Ico id={id} />{label}
      {id === 'load' && ws.files.length
        ? <span className="nav-badge">{ws.files.length}</span> : null}
    </button>
  )

  return (
    <div className="shell">
      <aside className="side">
        <div className="brand">
          <div className="brand-mark">P⇄A</div>
          <div>
            <div className="brand-name">PDI <em>Migration</em></div>
            <div className="brand-sub">Airflow Studio</div>
          </div>
          {version && <span className="version-pill">v{version}</span>}
        </div>
        <nav className="nav">
          <div className="nav-label">Overview</div>
          {navItem('home', 'Home')}
          <div className="nav-label">Migrate</div>
          {navItem('load', 'Load')}
          {navItem('configure', 'Configure')}
          {navItem('preview', 'Preview')}
          {navItem('deploy', 'Deploy')}
          <div className="nav-label">Observe</div>
          {navItem('pdigraph', 'PDI Graph')}
          {navItem('lineage', 'Lineage')}
          {navItem('reconcile', 'Reconcile')}
          <div className="nav-label">Configure</div>
          {navItem('settings', 'Settings')}
        </nav>
        <div className="side-foot">
          <div className="conn">
            <span className={`dot ${airflow?.reachable ? 'ok' : 'warn'}`} />
            {airflow?.reachable
              ? <>Airflow · <a href={airflow.url} target="_blank" rel="noreferrer">connected</a></>
              : 'Airflow · offline'}
          </div>
          <div className="conn">
            <span className={`dot ${carte?.reachable ? (carte?.authenticated ? 'ok' : 'warn') : 'warn'}`} />
            {carte?.reachable
              ? <>Carte · <a href={carte.url} target="_blank" rel="noreferrer">{carte.authenticated ? 'connected' : 'reachable'}</a></>
              : 'Carte · offline'}
          </div>
          <div className="conn">
            <span className={`dot ${marquez?.reachable ? 'ok' : 'warn'}`} />
            {marquez?.reachable
              ? <>Marquez · <a href={marquez.web_url} target="_blank" rel="noreferrer">connected</a></>
              : 'Marquez · offline'}
          </div>
          <div className="conn">
            <span className={`dot ${pdc?.reachable ? (pdc?.authenticated ? 'ok' : 'warn') : 'warn'}`} />
            {pdc?.reachable
              ? <>PDC · <a href={pdc.url} target="_blank" rel="noreferrer">{pdc.authenticated ? 'connected' : 'reachable'}</a></>
              : 'PDC · offline'}
          </div>
          <div className="conn">
            <a href="/docs" target="_blank" rel="noreferrer">API docs</a>
            {' · '}
            <a href="/redoc" target="_blank" rel="noreferrer">ReDoc</a>
            {pdc?.url ? <>{' · '}<a href={pdc.url} target="_blank" rel="noreferrer">PDC</a></> : null}
          </div>
          <ThemeSelect />
        </div>
      </aside>
      <div className="main">
        <div className="topbar">
          <span className="crumb">
            {crumbs.map((c, i) => (
              i === crumbs.length - 1
                ? <b key={c}>{c}</b>
                : <span key={c}>{c} / </span>
            ))}
          </span>
          <span className="topbar-spacer" />
        </div>
        <div className="content">
          {inWorkflow && <Stepper page={page} onNavigate={setPage} />}
          <Page onNavigate={setPage} airflow={airflow} />
        </div>
      </div>
    </div>
  )
}
