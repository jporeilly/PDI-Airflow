import { useEffect, useState } from 'react'
import { apiPost } from './../api.js'
import { useStudio } from './../state.js'

/* The view Marquez can't do: Job -> Transformations -> Steps as one
   nested, expandable graph. Layout is a simple longest-path layering
   computed from the deps — no graph library needed. */

const CARD_W = 216
const GAP_X = 64
const GAP_Y = 18
const H0 = 58            // collapsed card height
const STEP_H = 24

const STATE_BADGE = {
  COMPLETED: 'good', RUNNING: 'accent', FAILED: 'serious', ABORTED: 'warning',
}

function layers(entries) {
  const depth = {}
  const get = (id) => {
    if (id in depth) return depth[id]
    const e = entries.find((x) => x.id === id)
    depth[id] = e && e.deps.length
      ? Math.max(...e.deps.map(get)) + 1
      : 0
    return depth[id]
  }
  entries.forEach((e) => get(e.id))
  return depth
}

function topoSteps(steps) {
  const upstream = {}
  for (const e of steps.edges) {
    upstream[e.to] = (upstream[e.to] ?? 0) + 1
  }
  const order = []
  const queue = steps.nodes.filter((n) => !upstream[n.id])
  const remaining = new Map(Object.entries(upstream))
  const byId = Object.fromEntries(steps.nodes.map((n) => [n.id, n]))
  while (queue.length) {
    const n = queue.shift()
    order.push(n)
    for (const e of steps.edges.filter((x) => x.from === n.id)) {
      remaining.set(e.to, remaining.get(e.to) - 1)
      if (remaining.get(e.to) === 0) queue.push(byId[e.to])
    }
  }
  return order.length === steps.nodes.length ? order : steps.nodes
}

function StateBadge({ state }) {
  if (!state) return null
  return <span className={`badge ${STATE_BADGE[state] ?? 'neutral'}`}>{state.toLowerCase()}</span>
}

function JobGraph({ job, expanded, onToggle }) {
  const depth = layers(job.entries)
  const cols = {}
  for (const e of job.entries) {
    (cols[depth[e.id]] ??= []).push(e)
  }

  const heightOf = (e) => expanded[key(job, e)] && e.steps
    ? H0 + 10 + topoSteps(e.steps).length * STEP_H + 8
    : H0

  // Position cards: x by layer, y stacked per column
  const pos = {}
  let maxH = 0
  const nCols = Object.keys(cols).length
  for (const [layer, list] of Object.entries(cols)) {
    let y = 8
    for (const e of list) {
      pos[e.id] = { x: 8 + Number(layer) * (CARD_W + GAP_X), y }
      y += heightOf(e) + GAP_Y
    }
    maxH = Math.max(maxH, y)
  }
  const width = 16 + nCols * (CARD_W + GAP_X) - GAP_X
  const height = maxH + 8

  const edges = job.entries.flatMap((e) =>
    e.deps.map((d) => ({ from: d, to: e.id })))

  return (
    <div className="pg-wrap" style={{ height, minWidth: width }}>
      <svg className="pg-edges" width={width} height={height}>
        {edges.map((ed, i) => {
          const a = pos[ed.from]
          const b = pos[ed.to]
          if (!a || !b) return null
          const x1 = a.x + CARD_W
          const y1 = a.y + H0 / 2
          const x2 = b.x
          const y2 = b.y + H0 / 2
          const mx = (x1 + x2) / 2
          return (
            <path key={i}
              d={`M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`}
              className="pg-edge" markerEnd="url(#pg-arrow)" />
          )
        })}
        <defs>
          <marker id="pg-arrow" viewBox="0 0 8 8" refX="7" refY="4"
            markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M0 0 8 4 0 8Z" fill="currentColor" />
          </marker>
        </defs>
      </svg>
      {job.entries.map((e) => {
        const p = pos[e.id]
        const k = key(job, e)
        const open = expanded[k] && e.steps
        return (
          <div key={e.id} className="pg-card"
            style={{ left: p.x, top: p.y, width: CARD_W }}>
            <div className="pg-head"
              onClick={() => e.steps && onToggle(k)}
              style={{ cursor: e.steps ? 'pointer' : 'default' }}>
              <span className="pg-name" title={e.path || e.name}>{e.name}</span>
              <span className={`badge ${e.type === 'TRANS' ? 'accent' : 'neutral'}`}>
                {e.type === 'TRANS' ? 'trans' : 'job'}
              </span>
              <StateBadge state={e.state} />
              {e.steps && <span className="pg-caret">{open ? '▾' : '▸'}</span>}
            </div>
            {!e.steps && e.type === 'TRANS' && (
              <div className="pg-substate">no .ktr loaded — steps hidden</div>
            )}
            {open && (
              <div className="pg-steps">
                {topoSteps(e.steps).map((s, i) => (
                  <div key={s.id} className="pg-step">
                    <span className="pg-step-arrow">{i > 0 ? '↓' : ''}</span>
                    <span className="pg-step-name" title={s.step_type}>{s.name}</span>
                    <span className="pg-step-type">{s.step_type}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

const key = (job, e) => `${job.name}/${e.id}`

export default function PdiGraphPage({ onNavigate }) {
  const ws = useStudio()
  const [graph, setGraph] = useState(null)
  const [error, setError] = useState('')
  const [expanded, setExpanded] = useState({})

  const parsed = ws.files.filter((f) => f.doc)

  async function load() {
    setError('')
    try {
      const g = await apiPost('/api/pdi/graph', {
        files: parsed.map((f) => ({ filename: f.filename, content: f.content })),
      })
      setGraph(g)
      // Default: expand every transformation that has steps
      const open = {}
      for (const job of g.jobs) {
        for (const e of job.entries) {
          if (e.steps) open[`${job.name}/${e.id}`] = true
        }
      }
      setExpanded(open)
    } catch (err) {
      setError(err.message)
    }
  }

  useEffect(() => {
    if (parsed.length) load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ws.files])

  if (parsed.length === 0) {
    return (
      <>
        <div className="page-head">
          <h1>PDI Graph</h1>
          <p className="psub">Jobs, their transformations, and the steps inside — one nested view.</p>
        </div>
        <section className="card">
          <p className="hint-line">
            Load a .kjb (and its .ktr files, for step detail) first.
          </p>
          <div className="actions">
            <button className="primary" onClick={() => onNavigate('load')}>← Go to Load</button>
          </div>
        </section>
      </>
    )
  }

  return (
    <>
      <div className="page-head">
        <h1>PDI Graph</h1>
        <p className="psub">
          Click a transformation card to fold/unfold its steps. Run states
          overlay from Marquez when the pipeline has executed.
        </p>
      </div>

      {error && <div className="error">{error}</div>}
      {!graph && !error && <div className="loading">Building graph…</div>}

      {graph?.jobs.map((job) => (
        <section className="card" key={job.name}>
          <header>
            <h2>Job: {job.name} <span>{job.path}</span></h2>
          </header>
          <div className="table-scroll">
            <JobGraph job={job} expanded={expanded}
              onToggle={(k) => setExpanded((p) => ({ ...p, [k]: !p[k] }))} />
          </div>
        </section>
      ))}

      {graph?.transformations.map((t) => (
        <section className="card" key={t.name}>
          <header><h2>Transformation: {t.name} <span>standalone</span></h2></header>
          <div className="pg-steps pg-steps-flat">
            {topoSteps(t.steps).map((s, i) => (
              <div key={s.id} className="pg-step">
                <span className="pg-step-arrow">{i > 0 ? '↓' : ''}</span>
                <span className="pg-step-name">{s.name}</span>
                <span className="pg-step-type">{s.step_type}</span>
                <StateBadge state={s.state} />
              </div>
            ))}
          </div>
        </section>
      ))}

      <div className="actions">
        <button className="ghost" onClick={load}>Refresh run states</button>
      </div>
    </>
  )
}
