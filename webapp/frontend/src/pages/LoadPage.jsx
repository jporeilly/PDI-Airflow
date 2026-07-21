import { useRef, useState } from 'react'
import { apiPost } from './../api.js'
import {
  addFile, clearFiles, loadedTransNames, removeFile, requiredTransNames,
  rootFiles, setFileDoc, setFileRepoPath, useStudio,
} from './../state.js'

// Load PDI files — drag & drop (or browse), batch-aware. Jobs list the
// transformations they require and whether each has been dropped too.
export default function LoadPage({ onNavigate }) {
  const ws = useStudio()
  const [dragOver, setDragOver] = useState(false)
  const [busy, setBusy] = useState(false)
  const inputRef = useRef(null)

  async function ingest(fileList) {
    const files = [...fileList].filter((f) =>
      /\.(kjb|ktr|xml)$/i.test(f.name))
    if (!files.length) return
    setBusy(true)
    for (const file of files) {
      const content = await file.text()
      const id = addFile(file.name, content)
      try {
        const doc = await apiPost('/api/inspect',
          { filename: file.name, content })
        setFileDoc(id, doc)
      } catch (err) {
        setFileDoc(id, null, err.message)
      }
    }
    setBusy(false)
  }

  function onDrop(e) {
    e.preventDefault()
    setDragOver(false)
    ingest(e.dataTransfer.files)
  }

  const transLoaded = loadedTransNames(ws.files)
  const roots = rootFiles(ws.files)
  const parsed = ws.files.filter((f) => f.doc)
  const missingAny = parsed.some((f) =>
    f.doc.kind === 'job' &&
    requiredTransNames(f.doc).some((n) => !transLoaded.has(n)))

  return (
    <>
      <div className="page-head">
        <h1>Load PDI files</h1>
        <p className="psub">
          Drop one file or a whole batch. Each job (.kjb) or standalone
          transformation (.ktr) becomes a DAG; transformations referenced
          by a job are used for validation and step-level lineage.
        </p>
      </div>

      <section className="card">
        <header><h2>PDI files</h2></header>
        <div
          className={`dropzone${dragOver ? ' over' : ''}`}
          onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
          onDragLeave={() => setDragOver(false)}
          onDrop={onDrop}
          onClick={() => inputRef.current?.click()}
          role="button"
          tabIndex={0}
        >
          <div className="dz-title">Drag &amp; drop .kjb / .ktr files here</div>
          <div className="dz-hint">or click to browse — multiple files allowed</div>
          <input
            ref={inputRef}
            type="file"
            accept=".kjb,.ktr,.xml"
            multiple
            style={{ display: 'none' }}
            onChange={(e) => { ingest(e.target.files); e.target.value = '' }}
          />
        </div>
        {busy && <div className="loading">Parsing…</div>}

        {ws.files.length > 0 && (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>File</th><th>Kind</th><th>Repo path</th><th>Becomes</th>
                  <th>Required transformations</th><th></th>
                </tr>
              </thead>
              <tbody>
                {ws.files.map((f) => {
                  const isRoot = roots.includes(f)
                  const required = f.doc ? requiredTransNames(f.doc) : []
                  return (
                    <tr key={f.id}>
                      <td>{f.filename}</td>
                      <td>
                        {f.error
                          ? <span className="badge serious">parse error</span>
                          : f.doc
                            ? f.doc.kind
                            : <span className="badge neutral">…</span>}
                      </td>
                      <td>
                        {f.doc && (
                          <input
                            className="mono"
                            style={{ width: '100%', minWidth: 160 }}
                            value={f.repoPath}
                            spellCheck={false}
                            placeholder="/CSCU/txn_report"
                            title={'Repository path Carte will run. Uploads '
                                 + 'carry no folder, so set this for anything '
                                 + 'not at the repository root.'}
                            onChange={(e) =>
                              setFileRepoPath(f.id, e.target.value)}
                          />
                        )}
                      </td>
                      <td>
                        {f.doc && (isRoot
                          ? <span className="badge accent">DAG: {f.doc.name}</span>
                          : <span className="badge neutral">dependency</span>)}
                      </td>
                      <td>
                        {f.error && <span className="hint-line">{f.error}</span>}
                        {required.length === 0 && f.doc?.kind === 'job' &&
                          <span className="hint-line">none</span>}
                        {required.map((name) => (
                          <span
                            key={name}
                            className={`badge ${transLoaded.has(name) ? 'good' : 'warning'}`}
                            style={{ marginRight: 6 }}
                          >
                            {name}{transLoaded.has(name) ? ' ✓' : ' missing'}
                          </span>
                        ))}
                      </td>
                      <td>
                        <button className="ghost" onClick={() => removeFile(f.id)}>
                          Remove
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
        {missingAny && (
          <p className="hint-line">
            <span className="badge warning">note</span> Missing
            transformations don't block migration — the generated tasks
            call them by repository path on Carte — but dropping them here
            enables validation and step-level lineage in Marquez.
          </p>
        )}
        {ws.files.length > 0 && (
          <div className="actions">
            <button className="ghost" onClick={clearFiles}>Clear all</button>
          </div>
        )}
      </section>

      {parsed.length > 0 && (
        <div className="actions">
          <span className="summary">
            <b>{roots.length}</b> DAG{roots.length === 1 ? '' : 's'} will be
            generated from <b>{parsed.length}</b> file{parsed.length === 1 ? '' : 's'}.
          </span>
          <button className="primary" onClick={() => onNavigate('configure')}>
            Continue to Configure →
          </button>
        </div>
      )}
    </>
  )
}
