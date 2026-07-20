// Module-level store for the migration workflow (PDC-suite pattern:
// no contexts, no router — pages read the snapshot and call mutators).
//
// Batch model: `files` holds every dropped .kjb/.ktr. Jobs are always
// DAG roots; a .ktr is a root only when no loaded job references it
// (otherwise it's a dependency, used for validation and step lineage).
import { useSyncExternalStore } from 'react'

const state = {
  files: [],        // {id, filename, content, doc, error}
  options: {
    schedule: '',
    conn_id: 'pdi_default',
    mode: 'auto',
    deferrable: true,
    poll_interval: 10,
    retries: 0,
    owner: 'pdi2dag',
    level: 'Basic',
    params: {},
  },
  results: [],      // {filename, dag_id, code, warnings}
  deployed: null,   // {items: [{dag_id, dag_file, activated, run_id, error}]}
  lineage: null,    // /api/lineage/publish result
}

const listeners = new Set()
let snapshot = { ...state }
let nextId = 1

function commit() {
  snapshot = { ...state }
  listeners.forEach((l) => l())
}

function subscribe(l) {
  listeners.add(l)
  return () => listeners.delete(l)
}

export const useStudio = () => useSyncExternalStore(subscribe, () => snapshot)

/* ---------- derived helpers (pure — take the snapshot) ---------- */

// Transformation names a job's TRANS entries require (repo-path stems).
export function requiredTransNames(doc) {
  if (!doc || doc.kind !== 'job') return []
  return doc.entries
    .filter((e) => e.type === 'TRANS' && e.path)
    .map((e) => e.path.split('/').pop())
}

// Names of every loaded transformation (parsed .ktr files).
export function loadedTransNames(files) {
  return new Set(files
    .filter((f) => f.doc?.kind === 'transformation')
    .map((f) => f.doc.name))
}

// Roots = files that become DAGs: all jobs + unreferenced transformations.
export function rootFiles(files) {
  const referenced = new Set()
  for (const f of files) {
    if (f.doc?.kind === 'job') {
      for (const name of requiredTransNames(f.doc)) referenced.add(name)
    }
  }
  return files.filter((f) =>
    f.doc && (f.doc.kind === 'job' || !referenced.has(f.doc.name)))
}

/* ---------- mutators ---------- */

export function addFile(filename, content) {
  const id = nextId++
  // Re-dropping a file replaces the previous copy
  state.files = state.files
    .filter((f) => f.filename !== filename)
    .concat({ id, filename, content, doc: null, error: '' })
  state.results = []
  state.deployed = null
  state.lineage = null
  commit()
  return id
}

export function setFileDoc(id, doc, error = '') {
  state.files = state.files.map((f) =>
    f.id === id ? { ...f, doc, error } : f)
  if (doc?.parameters?.length) {
    const params = { ...state.options.params }
    for (const p of doc.parameters) {
      if (!(p.name in params)) params[p.name] = p.default
    }
    state.options = { ...state.options, params }
  }
  commit()
}

export function removeFile(id) {
  state.files = state.files.filter((f) => f.id !== id)
  state.results = []
  state.deployed = null
  commit()
}

export function clearFiles() {
  state.files = []
  state.results = []
  state.deployed = null
  state.lineage = null
  commit()
}

export function setOptions(patch) {
  state.options = { ...state.options, ...patch }
  state.results = []
  commit()
}

export function setParam(name, value) {
  state.options = {
    ...state.options,
    params: { ...state.options.params, [name]: value },
  }
  state.results = []
  commit()
}

export function setResults(results) {
  state.results = results
  commit()
}

export function setDeployed(deployed) {
  state.deployed = deployed
  commit()
}

export function setLineage(lineage) {
  state.lineage = lineage
  commit()
}
