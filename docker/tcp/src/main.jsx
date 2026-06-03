import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from '../../../dashboards/tcp-throughput-explainer.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>
)
