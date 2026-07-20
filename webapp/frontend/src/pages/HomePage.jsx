import { useStudio } from './../state.js'

const WORKFLOW = [
  { n: 1, page: 'load', title: 'Load',
    text: 'Pick a PDI job (.kjb) or transformation (.ktr). The studio parses its entries, hops and named parameters — nothing is uploaded anywhere else.' },
  { n: 2, page: 'configure', title: 'Configure',
    text: 'Choose the cron schedule, wrap or explode mode, deferrable execution and parameter templating ({{ ds }} and friends).' },
  { n: 3, page: 'preview', title: 'Preview',
    text: 'Review the generated DAG code and the migration warnings — every PDI entry without an Airflow equivalent is called out explicitly.' },
  { n: 4, page: 'deploy', title: 'Deploy',
    text: 'Write the DAG into the dags folder, wait for the scheduler to parse it, unpause it with its schedule, and optionally trigger a first run.' },
]

/* Architecture diagram — inline SVG, styled by the suite variables. */
function ArchDiagram() {
  const box = { fill: 'var(--surface-2, #f4f7fa)', stroke: 'var(--border, #cfd8e3)', strokeWidth: 1.2, rx: 8 }
  const label = { fontSize: 12, fontWeight: 700, fill: 'currentColor' }
  const sub = { fontSize: 10, fill: 'currentColor', opacity: 0.65 }
  const arrow = { stroke: 'var(--accent, #1c7293)', strokeWidth: 1.6, fill: 'none', markerEnd: 'url(#arr)' }
  return (
    <svg viewBox="0 0 720 250" style={{ width: '100%', maxWidth: 840, display: 'block', margin: '4px auto' }}>
      <defs>
        <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0 0 8 4 0 8Z" fill="var(--accent, #1c7293)" />
        </marker>
      </defs>
      <rect x="8" y="106" width="118" height="56" {...box} />
      <text x="67" y="130" textAnchor="middle" {...label}>.kjb / .ktr</text>
      <text x="67" y="146" textAnchor="middle" {...sub}>PDI repository files</text>

      <rect x="168" y="106" width="146" height="56" {...box} />
      <text x="241" y="130" textAnchor="middle" {...label}>Migration Studio</text>
      <text x="241" y="146" textAnchor="middle" {...sub}>pdi2dag converter</text>

      <rect x="388" y="20" width="150" height="52" {...box} />
      <text x="463" y="42" textAnchor="middle" {...label}>Apache Airflow</text>
      <text x="463" y="58" textAnchor="middle" {...sub}>schedule + orchestrate</text>

      <rect x="580" y="20" width="132" height="52" {...box} />
      <text x="646" y="42" textAnchor="middle" {...label}>Marquez</text>
      <text x="646" y="58" textAnchor="middle" {...sub}>orchestration lineage</text>

      <rect x="388" y="108" width="150" height="52" {...box} />
      <text x="463" y="130" textAnchor="middle" {...label}>Carte / PDI</text>
      <text x="463" y="146" textAnchor="middle" {...sub}>runs the actual ETL</text>

      <rect x="388" y="192" width="150" height="52" {...box} />
      <text x="463" y="214" textAnchor="middle" {...label}>Pentaho Data Catalog</text>
      <text x="463" y="230" textAnchor="middle" {...sub}>ETL + table lineage</text>

      {/* run path */}
      <path d="M126 134h38" {...arrow} />
      <path d="M314 120 384 52" {...arrow} />
      <path d="M463 72v32" {...arrow} />
      {/* orchestration lineage */}
      <path d="M538 46h38" {...arrow} />
      {/* PDI lineage: Studio -> PDC (below Carte) */}
      <path d="M300 162 C 340 218, 360 218, 384 218" {...arrow} />
      <text x="330" y="200" textAnchor="middle" {...sub}>PDI lineage</text>
      {/* Carte runtime row counts enrich the emitter (dashed) */}
      <path d="M388 140 C 350 150, 340 150, 316 146" stroke="var(--accent, #1c7293)" strokeWidth="1.2" strokeDasharray="3 3" fill="none" markerEnd="url(#arr)" />
    </svg>
  )
}

export default function HomePage({ onNavigate, airflow }) {
  const ws = useStudio()
  return (
    <>
      <div className="page-head">
        <h1>Migrate PDI jobs to scheduled Airflow DAGs</h1>
        <p className="psub">
          Convert a Pentaho Data Integration job or transformation into an
          Airflow DAG, deploy it with a schedule, and watch it run — with
          lineage in Marquez.
        </p>
      </div>

      <section className="card">
        <header><h2>How it fits together</h2></header>
        <ArchDiagram />
        <p className="hint-line">
          The generated DAG delegates execution to Carte via the
          airflow-provider-pentaho operators — Airflow owns scheduling,
          dependencies, retries and observability; PDI keeps doing the data
          work. Airflow emits orchestration lineage to Marquez, and the
          pdi2dag emitter publishes PDI table lineage (enriched with Carte
          run row counts — the dashed arrow) to Pentaho Data Catalog.
        </p>
      </section>

      <section className="card">
        <header><h2>The workflow <span>click a step to jump there</span></h2></header>
        <div className="grid-2">
          {WORKFLOW.map((s) => (
            <div className="tile" key={s.n}>
              <div className="bucket-title"><span className="dot-num">{s.n}</span> {s.title}</div>
              <p className="hint-line">{s.text}</p>
              <button className="ghost" onClick={() => onNavigate(s.page)}>
                Go to {s.title} →
              </button>
            </div>
          ))}
        </div>
      </section>

      <section className="card">
        <header><h2>Status</h2></header>
        <div className="tiles">
          <div className="tile">
            <div className="value">{ws.files.length || '—'}</div>
            <div className="label">Loaded PDI files</div>
          </div>
          <div className="tile">
            <div className="value">{airflow?.reachable ? 'connected' : 'offline'}</div>
            <div className="label">Airflow</div>
          </div>
          <div className="tile">
            <div className="value">{airflow?.dag_count ?? '—'}</div>
            <div className="label">DAGs visible</div>
          </div>
        </div>
      </section>
    </>
  )
}
