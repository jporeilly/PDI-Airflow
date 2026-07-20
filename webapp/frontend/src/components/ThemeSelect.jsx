import { useEffect, useState } from 'react'

const THEMES = [
  { id: 'midnight', label: 'Midnight' },
  { id: 'slate', label: 'Slate' },
  { id: 'pentaho', label: 'Pentaho' },
  { id: 'light', label: 'Light' },
]

export default function ThemeSelect() {
  const [theme, setTheme] = useState(
    () => localStorage.getItem('mc-theme') ?? 'light',
  )

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    localStorage.setItem('mc-theme', theme)
  }, [theme])

  return (
    <select
      className="text"
      value={theme}
      onChange={(e) => setTheme(e.target.value)}
      aria-label="Color theme"
    >
      {THEMES.map((t) => (
        <option key={t.id} value={t.id}>{t.label}</option>
      ))}
    </select>
  )
}
