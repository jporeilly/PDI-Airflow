// Minimal markdown rendering — headings, bullets, bold, inline code, links.
// Only used for our own generated content, so it doesn't need to be a full parser.
function inline(text, keyBase) {
  const parts = []
  const re = /(\*\*(.+?)\*\*|`(.+?)`|\[(.+?)\]\((.+?)\))/g
  let last = 0
  let m
  let i = 0
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) parts.push(text.slice(last, m.index))
    if (m[2]) parts.push(<strong key={`${keyBase}-${i}`}>{m[2]}</strong>)
    else if (m[3]) parts.push(<code key={`${keyBase}-${i}`}>{m[3]}</code>)
    else if (m[4]) parts.push(<a key={`${keyBase}-${i}`} href={m[5]} target="_blank" rel="noreferrer">{m[4]}</a>)
    last = m.index + m[0].length
    i++
  }
  if (last < text.length) parts.push(text.slice(last))
  return parts
}

export default function Markdown({ text }) {
  const blocks = []
  text.split('\n').forEach((line, n) => {
    if (line.startsWith('### ')) blocks.push(<h4 key={n}>{inline(line.slice(4), n)}</h4>)
    else if (line.startsWith('## ')) blocks.push(<h3 key={n}>{inline(line.slice(3), n)}</h3>)
    else if (line.startsWith('# ')) blocks.push(<h2 key={n}>{inline(line.slice(2), n)}</h2>)
    else if (line.startsWith('- ')) blocks.push(<li key={n}>{inline(line.slice(2), n)}</li>)
    else if (line.trim()) blocks.push(<p key={n}>{inline(line, n)}</p>)
  })
  return <div className="md">{blocks}</div>
}
