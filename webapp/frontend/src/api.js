// Fetch wrapper for the Migration Studio backend (webapp/backend/main.py).
// Every error path there returns {"error": msg} — never FastAPI's {"detail"} —
// so that is the one shape we surface.

async function request(path, options = {}) {
  let res
  try {
    res = await fetch(path, options)
  } catch {
    throw new Error('Cannot reach the Migration Studio backend.')
  }
  const ct = res.headers.get('content-type') || ''
  const body = ct.includes('json') ? await res.json().catch(() => null) : null
  if (!res.ok) throw new Error((body && body.error) || `${res.status} ${res.statusText}`)
  if (body && body.error) throw new Error(body.error)
  return body
}

export const apiGet = (path) => request(path)
export const apiPost = (path, body) =>
  request(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  })

/* Background jobs — POST /api/jobs/{name} -> job dict {id, status, phase,
   detail, done, total, events[], result}; poll GET /api/jobs/{id} until
   status leaves 'running'. onTick receives every fresh job dict. */
export async function runJob(name, body, onTick) {
  let job = await apiPost(`/api/jobs/${name}`, body)
  onTick?.(job)
  while (job.status === 'running') {
    await new Promise((r) => setTimeout(r, 1200))
    job = await apiGet(`/api/jobs/${job.id}`)
    onTick?.(job)
  }
  if (job.status === 'error') throw new Error(job.detail || 'Job failed')
  return job
}
