import React from 'react'
import { createRoot } from 'react-dom/client'
import App from './App.jsx'
import './index.css'

const saved = localStorage.getItem('mc-theme') ?? 'light'
document.documentElement.dataset.theme = saved

createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
